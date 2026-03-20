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
	ctx    context.Context
	log    component.Logger
	driver Driver

	minIp     net.IP
	maxIp     net.IP
	serviceIp net.IP

	defaultNet  NetworkSpec
	reservedNet NetworkSpec

	networks   map[*net.IP]*NetworkSpec
	networksMu sync.Mutex
}

func NewIpam(ctx context.Context, driver Driver) (Ipam, error) {
	log := component.NewLogger("ipam")

	defaultNet := driver.Networks().DefaultNetwork(ctx)
	if defaultNet.Type != NetworkBridge {
		return nil, fmt.Errorf("unsupported default network type %q", defaultNet.Type)
	}

	// Create a network to reserve an IP range for sandboxes with host ports.
	reservedNet, err := driver.Networks().CreateNetwork(ctx, "reserved", nil, nil)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create reserved network, trying to inspect it in case it already exists")
		return nil, component.WrapErr(log, err)
	}
	err = driver.Networks().RemoveNetwork(ctx, "reserved")
	if err != nil {
		return nil, component.WrapErr(log, err)
	}

	// Reserve a {Reservation.GatewayIP}/30 for static pods. This also gives our static pod a predictable IP address. A /30 gives us 1 usable IP.
	var serviceIp net.IP
	ipNet := reservedNet.Network
	ipNet.Mask = net.CIDRMask(30, 32)
	gateway := make(net.IP, len(ipNet.IP))
	copy(gateway, ipNet.IP)
	for i := range gateway {
		gateway[i] |= ^ipNet.Mask[i]
	}
	gateway[len(gateway)-1]-- // broadcast - 1
	staticNet, err := driver.Networks().CreateNetwork(ctx, "static", &ipNet, &gateway)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create static network, trying to inspect it in case it already exists")
		return nil, component.WrapErr(log, err)
	}
	// static IP is the gateway IP + 1, which is the first usable IP in the reserved /30 subnet.
	// example: 172.18.0.0/30 → static IP: 172.18.0.2
	serviceIp = make(net.IP, len(staticNet.Network.IP))
	copy(serviceIp, staticNet.Network.IP)
	serviceIp[len(serviceIp)-1] += 2

	// minIp: gateway IP of the default network
	// maxIp: broadcast IP of the reserved network (the last IP in the reserved range)
	// examples:
	// - defaultNet: 172.17.0.0/16, reservedNet: 172.18.0.0/16 → minIp: 172.17.0.1, maxIp: 172.18.255.255
	// - defaultNet: 172.17.0.0/16, reservedNet: 172.25.0.0/16 → minIp: 172.17.0.1, maxIp: 172.25.255.255
	minIp := make(net.IP, len(defaultNet.Network.IP))
	copy(minIp, defaultNet.Network.IP)
	minIp[len(minIp)-1] |= 1
	maxIp := make(net.IP, len(reservedNet.Network.IP))
	copy(maxIp, reservedNet.Network.IP)
	for i := range maxIp {
		maxIp[i] |= ^reservedNet.Network.Mask[i]
	}

	log.Info().Str("defaultNetwork", defaultNet.Name).Str("defaultSubnet", defaultNet.Network.String()).Str("reservedSubnet", reservedNet.Network.String()).Str("serviceIP", serviceIp.String()).Str("minIP", minIp.String()).Str("maxIP", maxIp.String()).Msg("IPAM initialized")

	return &IpamImpl{
		ctx:         ctx,
		log:         log,
		driver:      driver,
		minIp:       minIp,
		maxIp:       maxIp,
		serviceIp:   serviceIp,
		defaultNet:  defaultNet,
		reservedNet: reservedNet,
		networks:    map[*net.IP]*NetworkSpec{&staticNet.Network.IP: &staticNet},
	}, nil
}

func (i *IpamImpl) ServiceIp() net.IP {
	// Used for the control plane static pod.
	return i.serviceIp
}

func (i *IpamImpl) ServiceNet() *net.IPNet {
	return &i.reservedNet.Network
}

func (i *IpamImpl) AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error) {
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

		// Check if the next subnet exceeds the max IP.
		broadcast := make(net.IP, len(nextNet.IP))
		copy(broadcast, nextNet.IP)
		for j := range broadcast {
			broadcast[j] |= ^nextNet.Mask[j]
		}
		if bytes.Compare(broadcast, i.maxIp) > 0 {
			return nil, fmt.Errorf("no more available subnets for static pods")
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
