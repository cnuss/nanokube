package nanokube

import (
	"context"
	"net"
	"sync"

	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type NetworkImpl struct {
	service v1.NetworkService

	defaultNet     v1.AllocatedNetwork
	defaultNetOnce sync.Once

	networks sync.Map // map[string]AllocatedNetwork
	nextIP   net.IP
	nextIPMu sync.Mutex
}

var _ v1.Network = &NetworkImpl{}

func NewNetwork(service v1.NetworkService) v1.Network {
	return &NetworkImpl{service: service}
}

func (n *NetworkImpl) Default() v1.AllocatedNetwork {
	n.defaultNetOnce.Do(func() {
		var err error

		err = n.service.CreateNetwork(n.service.Context(), "reserved", "bridge", nil, nil)
		if err != nil {
			n.defaultNet, _ = newAllocatedNetwork(n.service.DefaultNetwork(n.service.Context()), n)
			return
		}
		_, _, _, reservedNet, err := n.service.GetNetwork(n.service.Context(), "reserved")
		if err != nil {
			n.defaultNet, _ = newAllocatedNetwork(n.service.DefaultNetwork(n.service.Context()), n)
			return
		}
		err = n.service.RemoveNetwork(n.service.Context(), "reserved")
		if err != nil {
			n.defaultNet, _ = newAllocatedNetwork(n.service.DefaultNetwork(n.service.Context()), n)
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

		err = n.service.CreateNetwork(n.service.Context(), "static", "bridge", ipNet, &gateway)
		if err != nil {
			n.defaultNet, _ = newAllocatedNetwork(n.service.DefaultNetwork(n.service.Context()), n)
			return
		}

		n.defaultNet, err = newAllocatedNetwork("static", n)
		if err != nil {
			n.defaultNet, _ = newAllocatedNetwork(n.service.DefaultNetwork(n.service.Context()), n)
			return
		}

		// nextIP = first IP past the static network
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
		klog.Fatalf("default network not initialized")
	}

	return n.defaultNet
}

func (n *NetworkImpl) Get(status *criv1.PodSandboxStatus) (v1.AllocatedNetwork, error) {
	if status.GetNetwork() == nil {
		return nil, nil
	}
	return newAllocatedNetwork(status.GetMetadata().GetName(), n)
}

func (n *NetworkImpl) Allocate(config *criv1.PodSandboxConfig) (v1.AllocatedNetwork, error) {
	if config != nil && config.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		return n.Default(), nil
	}

	n.nextIPMu.Lock()
	defer n.nextIPMu.Unlock()

	var nextNet *net.IPNet
	var reclaimKey string
	n.networks.Range(func(key, value any) bool {
		if value == nil {
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

	err := n.service.CreateNetwork(n.service.Context(), config.GetMetadata().GetName(), "bridge", nextNet, nil)
	if err != nil {
		return nil, err
	}

	network, err := newAllocatedNetwork(config.GetMetadata().GetName(), n)
	if err != nil {
		return nil, err
	}

	if reclaimKey != "" {
		n.networks.Store(reclaimKey, network)
	} else {
		n.networks.Store(nextNet.IP.String(), network)
		// Advance high water mark
		carry := uint32(1) << uint(32-v1.NetworkSubnetSize)
		for j := len(n.nextIP) - 1; j >= 0 && carry > 0; j-- {
			sum := uint32(n.nextIP[j]) + carry
			n.nextIP[j] = byte(sum)
			carry = sum >> 8
		}
	}

	return network, nil
}

type allocatedNetworkImpl struct {
	network     *NetworkImpl
	name        string
	networkType v1.NetworkType
	gateway     net.IP
	net         net.IPNet
}

func newAllocatedNetwork(name string, network *NetworkImpl) (v1.AllocatedNetwork, error) {
	resolvedName, networkType, gateway, net, err := network.service.GetNetwork(network.service.Context(), name)
	if err != nil {
		return nil, err
	}
	n := name
	if resolvedName != nil {
		n = *resolvedName
	}
	return &allocatedNetworkImpl{name: n, network: network, networkType: *networkType, gateway: *gateway, net: *net}, nil
}

func (a *allocatedNetworkImpl) Name() string {
	return a.name
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

func (a *allocatedNetworkImpl) Deallocate(ctx context.Context) error {
	ipKey := a.net.IP.String()
	if _, ok := a.network.networks.Load(ipKey); !ok {
		return nil
	}
	err := a.network.service.RemoveNetwork(ctx, a.Name())
	if err != nil {
		return err
	}
	a.network.networks.Store(ipKey, nil) // preserve slot for reclamation
	return nil
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
