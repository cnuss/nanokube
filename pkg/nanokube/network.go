package nanokube

import (
	"context"
	"fmt"
	"net"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type NetworkImpl struct {
	kubelet v1.Kubelet
	service v1.NetworkService

	defaultNet     v1.AllocatedNetwork
	defaultNetOnce sync.Once

	networks sync.Map // map[string]v1.AllocatedNetwork

	nextIP   net.IP
	nextIPMu sync.Mutex
}

var _ v1.Network = &NetworkImpl{}

func NewNetwork(kubelet v1.Kubelet, service v1.NetworkService) v1.Network {
	return &NetworkImpl{
		kubelet: kubelet,
		service: service,
	}
}

func (n *NetworkImpl) Default(ctx context.Context) v1.AllocatedNetwork {
	n.defaultNetOnce.Do(func() {
		var err error

		id, err := n.service.CreateNetwork(ctx, "bridge", nil, nil)
		if err != nil {
			n.kubelet.Cancel(NewError(fmt.Errorf("failed to reserve bridge network: %w", err)))
			return
		}
		_, _, reservedNet, err := n.service.GetNetwork(ctx, id)
		if err != nil {
			n.kubelet.Cancel(NewError(fmt.Errorf("failed to get reserved bridge network: %w", err)))
			return
		}
		err = n.service.RemoveNetwork(ctx, id)
		if err != nil {
			n.kubelet.Cancel(NewError(fmt.Errorf("failed to remove reserved bridge network: %w", err)))
			return
		}

		// Create static network with gateway pinned high so container gets first usable IP
		ipNet := reservedNet
		ipNet.Mask = net.CIDRMask(v1.NetworkSubnetSize, 32)
		gateway := make(net.IP, len(ipNet.IP))
		copy(gateway, ipNet.IP)
		for j := range gateway {
			gateway[j] |= ^ipNet.Mask[j]
		}
		for j := len(gateway) - 1; j >= 0; j-- {
			gateway[j]--
			if gateway[j] != 0xFF {
				break
			}
		}

		id, err = n.service.CreateNetwork(ctx, "bridge", ipNet, &gateway)
		if err != nil {
			n.kubelet.Cancel(NewError(fmt.Errorf("failed to create default bridge network: %w", err)))
			return
		}

		n.defaultNet, err = newAllocatedNetwork(ctx, id, n)
		if err != nil {
			n.kubelet.Cancel(NewError(fmt.Errorf("failed to create static network: %w", err)))
			return
		}

		// nextIP = first IP past the static network
		n.nextIPMu.Lock()
		defer n.nextIPMu.Unlock()
		ones, _ := ipNet.Mask.Size()
		n.nextIP = make(net.IP, len(ipNet.IP))
		copy(n.nextIP, ipNet.IP)
		carry := uint32(1) << uint(32-ones)
		for j := len(n.nextIP) - 1; j >= 0 && carry > 0; j-- {
			sum := uint32(n.nextIP[j]) + carry
			n.nextIP[j] = byte(sum)
			carry = sum >> 8
		}
	})

	if n.defaultNet == nil {
		n.kubelet.Cancel(NewError(fmt.Errorf("default network not initialized")))
		return nil
	}
	return n.defaultNet
}

func (n *NetworkImpl) Networks() []v1.AllocatedNetwork {
	var networks []v1.AllocatedNetwork
	n.networks.Range(func(key, value interface{}) bool {
		network, ok := value.(v1.AllocatedNetwork)
		if !ok {
			return true
		}
		if network != nil {
			networks = append(networks, network)
		}
		return true
	})
	return networks
}

func (n *NetworkImpl) FromID(ctx context.Context, id string) (v1.AllocatedNetwork, error) {
	var found v1.AllocatedNetwork
	n.networks.Range(func(key, value interface{}) bool {
		an, ok := value.(v1.AllocatedNetwork)
		if !ok || an == nil || an.ID() != id {
			return true
		}
		found = an
		return false
	})

	return found, nil
}

func (n *NetworkImpl) FromUID(ctx context.Context, sandboxUID types.UID) (v1.AllocatedNetwork, error) {
	var found v1.AllocatedNetwork
	n.networks.Range(func(key, value interface{}) bool {
		an, ok := value.(v1.AllocatedNetwork)
		if !ok || an == nil || an.SandboxUID() == nil || *an.SandboxUID() != sandboxUID {
			return true
		}
		found = an
		return false
	})

	return found, nil
}

func (n *NetworkImpl) FromIP(ctx context.Context, ip net.IP) (v1.AllocatedNetwork, error) {
	ipv4 := ip.To4()
	if ipv4 == nil {
		return nil, nil
	}
	ipKey := ipv4.Mask(net.CIDRMask(v1.NetworkSubnetSize, 32)).String()

	value, found := n.networks.Load(ipKey)
	if !found {
		return nil, nil
	}
	network, ok := value.(v1.AllocatedNetwork)
	if !ok || network == nil {
		return nil, nil
	}
	return network, nil
}

func (n *NetworkImpl) FromStatus(ctx context.Context, status *criv1.PodSandboxStatus) (v1.AllocatedNetwork, error) {
	if status == nil {
		return nil, nil
	}

	if status.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		network := n.Default(ctx)
		return network, nil
	}

	return n.FromIP(ctx, net.ParseIP(status.GetNetwork().GetIp()))
}

func (n *NetworkImpl) FromConfig(ctx context.Context, config *criv1.PodSandboxConfig) (v1.AllocatedNetwork, error) {
	sandboxUID := types.UID(config.GetMetadata().GetUid())

	if config != nil && config.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		return n.Default(ctx).WithSandboxUID(&sandboxUID), nil
	}

	n.nextIPMu.Lock()
	defer n.nextIPMu.Unlock()

	var nextNet *net.IPNet
	var reclaimKey string
	n.networks.Range(func(key, value interface{}) bool {
		// Find the first network without a sandbox UID to reclaim
		an := value.(v1.AllocatedNetwork)
		if an == nil || an.SandboxUID() == nil {
			nextNet = &net.IPNet{IP: net.ParseIP(key.(string)), Mask: net.CIDRMask(v1.NetworkSubnetSize, 32)}
			reclaimKey = key.(string)
			return false
		}
		return true
	})

	if nextNet == nil {
		nextNet = &net.IPNet{
			IP:   make(net.IP, len(n.nextIP)),
			Mask: net.CIDRMask(v1.NetworkSubnetSize, 32),
		}
		copy(nextNet.IP, n.nextIP)
	}

	id, err := n.service.CreateNetwork(ctx, "bridge", nextNet, nil)
	if err != nil {
		return nil, err
	}

	network, err := newAllocatedNetwork(ctx, id, n)
	if err != nil {
		return nil, err
	}

	if reclaimKey == "" {
		// Advance high water mark
		carry := uint32(1) << uint(32-v1.NetworkSubnetSize)
		for j := len(n.nextIP) - 1; j >= 0 && carry > 0; j-- {
			sum := uint32(n.nextIP[j]) + carry
			n.nextIP[j] = byte(sum)
			carry = sum >> 8
		}
	}

	return network.WithSandboxUID(&sandboxUID), nil
}

type allocatedNetworkImpl struct {
	network *NetworkImpl

	id          string
	networkType v1.NetworkType
	gateway     net.IP
	net         net.IPNet
	sandboxUID  *types.UID
}

var _ v1.AllocatedNetwork = &allocatedNetworkImpl{}

func newAllocatedNetwork(ctx context.Context, id string, network *NetworkImpl) (v1.AllocatedNetwork, error) {
	networkType, gateway, ipNet, err := network.service.GetNetwork(ctx, id)
	if err != nil {
		return nil, err
	}
	if networkType == nil || gateway == nil || ipNet == nil {
		return nil, fmt.Errorf("network %q not found", id)
	}
	ipKey := ipNet.IP.String()
	an, _ := network.networks.LoadOrStore(ipKey, v1.AllocatedNetwork(
		&allocatedNetworkImpl{
			id:          id,
			network:     network,
			networkType: *networkType,
			gateway:     *gateway,
			net:         *ipNet,
		}))
	return an.(v1.AllocatedNetwork), nil
}

func (a *allocatedNetworkImpl) ID() string {
	return a.id
}

func (a *allocatedNetworkImpl) Type() v1.NetworkType {
	return a.networkType
}

func (a *allocatedNetworkImpl) Gateway() net.IP {
	return a.gateway
}

func (a *allocatedNetworkImpl) Network() net.IPNet {
	return a.net
}

func (a *allocatedNetworkImpl) Deallocate() error {
	an := a.WithSandboxUID(nil)
	if an == nil {
		return fmt.Errorf("unknown network: %s", a.net.IP.String())
	}
	return nil
}

func (a *allocatedNetworkImpl) SandboxUID() *types.UID {
	return a.sandboxUID
}

func (a *allocatedNetworkImpl) WithSandboxUID(sandboxUID *types.UID) v1.AllocatedNetwork {
	if a.sandboxUID != nil && sandboxUID != nil && *a.sandboxUID != *sandboxUID {
		Log.Warn("attempt to change sandbox UID of allocated network", "networkID", a.id, "oldUID", *a.sandboxUID, "newUID", *sandboxUID)
		return nil
	}
	an, ok := a.network.networks.Swap(a.net.IP.String(), v1.AllocatedNetwork(&allocatedNetworkImpl{
		id:          a.id,
		network:     a.network,
		networkType: a.networkType,
		gateway:     a.gateway,
		net:         a.net,
		sandboxUID:  sandboxUID,
	}))
	if !ok {
		return nil
	}
	return an.(v1.AllocatedNetwork)
}

type portMapImpl struct {
	local    int32
	remote   int32
	protocol v1.Protocol
}

func (p *portMapImpl) Local() int32 {
	return p.local
}

func (p *portMapImpl) Remote() int32 {
	return p.remote
}

func (p *portMapImpl) Protocol() v1.Protocol {
	return p.protocol
}

func NewPortMap(local, remote int32, protocol v1.Protocol) v1.PortMap {
	return &portMapImpl{local: local, remote: remote, protocol: protocol}
}
