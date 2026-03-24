package backend

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	// SubnetSize is the CIDR prefix length for per-pod networks.
	SubnetSize = 30
	// StaticSubnetSize is the CIDR prefix length for the static service network.
	StaticSubnetSize = 28
)

type ServiceConfig struct {
	IP           net.IP
	Net          *net.IPNet
	StaticPodNet *NetworkSpec
}

type Ipam interface {
	Service() *ServiceConfig
	ServiceIp() net.IP
	ServiceNet() *net.IPNet
	AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error)
	DeallocateNetwork(ctx context.Context, status *runtimeapi.PodSandboxStatus) error
	staticPodNet() *NetworkSpec
}

type IpamImpl struct {
	log    component.Logger
	driver Driver

	service     *ServiceConfig
	serviceOnce sync.Once

	networks   map[*net.IP]*NetworkSpec
	networksMu sync.Mutex
}

func NewIpam(driver Driver) *IpamImpl {
	return &IpamImpl{
		log:      component.NewLogger("ipam"),
		driver:   driver,
		networks: map[*net.IP]*NetworkSpec{},
	}
}

func (i *IpamImpl) Service() *ServiceConfig {
	i.initService()
	return i.service
}

func (i *IpamImpl) ServiceIp() net.IP {
	return i.Service().IP
}

func (i *IpamImpl) ServiceNet() *net.IPNet {
	return i.Service().Net
}

func (i *IpamImpl) staticPodNet() *NetworkSpec {
	return i.Service().StaticPodNet
}

func (i *IpamImpl) initService() {
	i.serviceOnce.Do(func() {
		ctx := context.Background()

		// Discover default network
		defaultNet := i.driver.Networks().DefaultNetwork(ctx)

		// Create and remove a "reserved" network to discover the next available range
		reservedNet, err := i.driver.Networks().CreateNetwork(ctx, "reserved", nil, nil)
		if err != nil {
			i.log.Warn().Err(err).Msg("failed to create reserved network")
			return
		}
		err = i.driver.Networks().RemoveNetwork(ctx, "reserved")
		if err != nil {
			i.log.Warn().Err(err).Msg("failed to remove reserved network")
			return
		}

		// Create static network with gateway pinned high so container gets first usable IP
		ipNet := reservedNet.Network
		ipNet.Mask = net.CIDRMask(StaticSubnetSize, 32)
		gateway := make(net.IP, len(ipNet.IP))
		copy(gateway, ipNet.IP)
		for j := range gateway {
			gateway[j] |= ^ipNet.Mask[j]
		}
		// broadcast - 1 (last usable host IP)
		for j := len(gateway) - 1; j >= 0; j-- {
			gateway[j]--
			if gateway[j] != 0xFF {
				break
			}
		}

		staticPodNet, err := i.driver.Networks().CreateNetwork(ctx, "static-pods", &ipNet, &gateway)
		if err != nil {
			i.log.Warn().Err(err).Msg("failed to create static pods network")
			return
		}

		// Container gets the first usable IP (network base + 1), gateway is pinned high
		serviceIp := make(net.IP, len(staticPodNet.Network.IP))
		copy(serviceIp, staticPodNet.Network.IP)
		for j := len(serviceIp) - 1; j >= 0; j-- {
			serviceIp[j]++
			if serviceIp[j] != 0 {
				break
			}
		}

		// Service CIDR uses the static network's base IP with the default /16 mask
		serviceNet := net.IPNet{
			IP:   staticPodNet.Network.IP,
			Mask: reservedNet.Network.Mask,
		}
		i.service = &ServiceConfig{
			IP:           serviceIp,
			Net:          &serviceNet,
			StaticPodNet: &staticPodNet,
		}
		i.networks[&staticPodNet.Network.IP] = &staticPodNet

		i.log.Info().
			Str("defaultNetwork", defaultNet.Name).
			Str("defaultSubnet", defaultNet.Network.String()).
			Str("reservedSubnet", reservedNet.Network.String()).
			Str("serviceIP", serviceIp.String()).
			Str("staticPodNetwork", staticPodNet.Name).
			Msg("IPAM initialized")
	})
}

func (i *IpamImpl) AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error) {
	i.initService()
	i.log.Info().Str("pod", config.Metadata.Name).Msg("allocating network for pod")

	if config != nil && config.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", config.Metadata.Name).Msg("skipping network allocation for static pod")
		return i.staticPodNet(), nil
	}

	i.networksMu.Lock()
	defer i.networksMu.Unlock()

	// Try to reclaim a released slot first, otherwise allocate the next subnet.
	var nextNet *net.IPNet
	var reclaimKey *net.IP
	for ip, existing := range i.networks {
		if existing == nil {
			nextNet = &net.IPNet{IP: *ip, Mask: net.CIDRMask(SubnetSize, 32)}
			reclaimKey = ip
			break
		}
	}

	if nextNet == nil {
		// No released slots — compute the next subnet after the highest allocated one.
		// Each network may have a different prefix length (e.g., /24 static vs /30 pod),
		// so compute the end of each network using its own mask to find the true max.
		var maxEnd net.IP
		for _, existing := range i.networks {
			if existing == nil {
				continue
			}
			ones, _ := existing.Network.Mask.Size()
			end := make(net.IP, len(existing.Network.IP))
			copy(end, existing.Network.IP)
			carry := uint32(1) << uint(32-ones)
			for j := len(end) - 1; j >= 0 && carry > 0; j-- {
				sum := uint32(end[j]) + carry
				end[j] = byte(sum)
				carry = sum >> 8
			}
			if maxEnd == nil || bytes.Compare(end, maxEnd) > 0 {
				maxEnd = end
			}
		}

		if maxEnd == nil {
			return nil, fmt.Errorf("no networks allocated yet")
		}

		nextNet = &net.IPNet{
			IP:   maxEnd,
			Mask: net.CIDRMask(SubnetSize, 32),
		}
	}

	netSpec, err := i.driver.Networks().CreateNetwork(ctx, config.GetMetadata().GetName(), nextNet, nil)
	if err != nil {
		return nil, fmt.Errorf("create network for pod: %w", err)
	}

	if reclaimKey != nil {
		i.networks[reclaimKey] = &netSpec
	} else {
		i.networks[&nextNet.IP] = &netSpec
	}
	return &netSpec, nil
}

func (i *IpamImpl) DeallocateNetwork(ctx context.Context, status *runtimeapi.PodSandboxStatus) error {
	i.log.Info().Str("pod", status.GetMetadata().GetName()).Msg("deallocating network for pod")

	if status.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", status.GetMetadata().GetName()).Msg("skipping network deallocation for static pod")
		return nil
	}

	i.networksMu.Lock()
	defer i.networksMu.Unlock()

	err := i.driver.Networks().RemoveNetwork(ctx, status.GetMetadata().GetName())
	if err != nil {
		i.log.Debug().Err(err).Str("sandbox", status.GetMetadata().GetName()).Msg("failed to remove sandbox network")
	}

	for ip, net := range i.networks {
		if net.Name == status.GetMetadata().GetName() {
			i.networks[ip] = nil
			break
		}
	}

	return nil
}

var _ Ipam = &IpamImpl{}
