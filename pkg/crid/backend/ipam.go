package backend

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type Ipam interface {
	ServiceIp() net.IP
	ServiceNet() *net.IPNet
	AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error)
	DeallocateNetwork(ctx context.Context, status *runtimeapi.PodSandboxStatus) error
}

type IpamImpl struct {
	log    component.Logger
	driver Driver

	serviceIp net.IP

	defaultNet     NetworkSpec
	reservedNet    NetworkSpec
	reservedOnce   sync.Once
	serviceNetOnce sync.Once

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

// initReserved discovers the default network and reserves an IP range.
func (i *IpamImpl) initReserved() {
	i.reservedOnce.Do(func() {
		ctx := context.Background()

		defaultNet := i.driver.Networks().DefaultNetwork(ctx)
		i.defaultNet = defaultNet

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
		i.reservedNet = reservedNet

		i.log.Info().Str("defaultNetwork", defaultNet.Name).Str("defaultSubnet", defaultNet.Network.String()).Str("reservedSubnet", reservedNet.Network.String()).Msg("IPAM reserved range initialized")
	})
}

// initService creates the static /30 network for the control plane pod.
func (i *IpamImpl) initService() {
	i.serviceNetOnce.Do(func() {
		i.initReserved()
		ctx := context.Background()

		ipNet := i.reservedNet.Network
		ipNet.Mask = net.CIDRMask(30, 32)
		gateway := make(net.IP, len(ipNet.IP))
		copy(gateway, ipNet.IP)
		for j := range gateway {
			gateway[j] |= ^ipNet.Mask[j]
		}
		gateway[len(gateway)-1]-- // broadcast - 1

		staticNet, err := i.driver.Networks().CreateNetwork(ctx, "static", &ipNet, &gateway)
		if err != nil {
			i.log.Warn().Err(err).Msg("failed to create static network")
			return
		}

		// Container gets the first usable IP (.1), gateway is pinned high (.2)
		i.serviceIp = make(net.IP, len(staticNet.Network.IP))
		copy(i.serviceIp, staticNet.Network.IP)
		i.serviceIp[len(i.serviceIp)-1] += 1

		i.networks[&staticNet.Network.IP] = &staticNet

		i.log.Info().Str("serviceIP", i.serviceIp.String()).Msg("IPAM service network initialized")
	})
}

func (i *IpamImpl) ServiceIp() net.IP {
	i.initService()
	return i.serviceIp
}

func (i *IpamImpl) ServiceNet() *net.IPNet {
	i.initReserved()
	return &i.reservedNet.Network
}

func (i *IpamImpl) AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error) {
	i.initService()
	i.log.Info().Str("pod", config.Metadata.Name).Msg("allocating network for pod")

	if config.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", config.Metadata.Name).Msg("skipping network allocation for static pod")
		for _, net := range i.networks {
			return net, nil
		}
		return nil, fmt.Errorf("no static network available")
	}

	i.networksMu.Lock()
	defer i.networksMu.Unlock()

	// Try to reclaim a released slot first, otherwise allocate the next /30.
	var nextNet *net.IPNet
	var reclaimKey *net.IP
	for ip, existing := range i.networks {
		if existing == nil {
			nextNet = &net.IPNet{IP: *ip, Mask: net.CIDRMask(30, 32)}
			reclaimKey = ip
			break
		}
	}

	if nextNet == nil {
		// No released slots — compute the next /30 after the highest allocated one.
		var allocated []*NetworkSpec
		for _, existing := range i.networks {
			if existing != nil {
				allocated = append(allocated, existing)
			}
		}
		sort.Slice(allocated, func(a, b int) bool {
			return bytes.Compare(allocated[a].Network.IP, allocated[b].Network.IP) < 0
		})

		var last *NetworkSpec = &i.reservedNet
		if len(allocated) > 0 {
			last = allocated[len(allocated)-1]
		}

		nextNet = &net.IPNet{
			IP:   make(net.IP, len(last.Network.IP)),
			Mask: net.CIDRMask(30, 32),
		}
		copy(nextNet.IP, last.Network.IP)
		carry := uint16(4) // size of a /30 subnet
		for j := len(nextNet.IP) - 1; j >= 0 && carry > 0; j-- {
			sum := uint16(nextNet.IP[j]) + carry
			nextNet.IP[j] = byte(sum)
			carry = sum >> 8
		}

	}

	netSpec, err := i.driver.Networks().CreateNetwork(ctx, config.Metadata.Uid, nextNet, nil)
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
	i.log.Info().Str("pod", status.Metadata.Name).Msg("deallocating network for pod")

	if status.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", status.Metadata.Name).Msg("skipping network deallocation for static pod")
		return nil
	}

	i.networksMu.Lock()
	defer i.networksMu.Unlock()

	err := i.driver.Networks().RemoveNetwork(ctx, status.Metadata.Uid)
	if err != nil {
		i.log.Warn().Err(err).Str("sandbox", status.Metadata.Name).Msg("failed to remove sandbox network")
	}

	// Remove the network from the networks map.
	for ip, net := range i.networks {
		if net.Name == status.Metadata.Uid {
			i.networks[ip] = nil
			break
		}
	}

	return nil
}

var _ Ipam = &IpamImpl{}
