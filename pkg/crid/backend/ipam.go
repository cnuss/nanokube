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

type Ipam interface {
	StaticIP() net.IP
	StaticNetwork() NetworkSpec
	AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error)
	DeallocateNetwork(ctx context.Context, status *runtimeapi.PodSandboxStatus) error
}

type IpamImpl struct {
	ctx    context.Context
	log    component.Logger
	driver Driver

	minIp    net.IP
	maxIp    net.IP
	staticIp net.IP

	defaultNet  NetworkSpec
	reservedNet NetworkSpec

	staticNets   map[*net.IP]*NetworkSpec
	staticNetsMu sync.Mutex
}

func NewIpam(ctx context.Context, driver Driver) (Ipam, error) {
	log := component.NewLogger("ipam")

	defaultNet := driver.Networks().DefaultNetwork(ctx)
	if defaultNet.Type != NetworkBridge {
		return nil, fmt.Errorf("unsupported default network type %q", defaultNet.Type)
	}

	// Create a network to reserve an IP range for sandboxes with host ports.
	reservedNet, err := driver.Networks().CreateNetwork(ctx, "reserved", nil)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create reserved network, trying to inspect it in case it already exists")
		return nil, component.WrapErr(log, err)
	}
	err = driver.Networks().RemoveNetwork(ctx, "reserved")
	if err != nil {
		return nil, component.WrapErr(log, err)
	}

	// Reserve a {Reservation.GatewayIP}/30 for static pods. This also gives our static pod a predictable IP address. A /30 gives us 1 usable IP.
	var staticIp net.IP
	ipNet := reservedNet.Network
	ipNet.Mask = net.CIDRMask(30, 32)
	staticNet, err := driver.Networks().CreateNetwork(ctx, "static", &ipNet)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create static network, trying to inspect it in case it already exists")
		return nil, component.WrapErr(log, err)
	}
	// static IP is the gateway IP + 1, which is the first usable IP in the reserved /30 subnet.
	// example: 172.18.0.0/30 → static IP: 172.18.0.2
	staticIp = make(net.IP, len(staticNet.Network.IP))
	copy(staticIp, staticNet.Network.IP)
	staticIp[len(staticIp)-1] += 2

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

	log.Info().Str("defaultNetwork", defaultNet.Name).Str("defaultSubnet", defaultNet.Network.String()).Str("reservedSubnet", reservedNet.Network.String()).Str("staticIP", staticIp.String()).Str("minIP", minIp.String()).Str("maxIP", maxIp.String()).Msg("IPAM initialized")

	return &IpamImpl{
		ctx:         ctx,
		log:         log,
		driver:      driver,
		minIp:       minIp,
		maxIp:       maxIp,
		staticIp:    staticIp,
		defaultNet:  defaultNet,
		reservedNet: reservedNet,
		staticNets:  map[*net.IP]*NetworkSpec{&staticNet.Network.IP: &staticNet},
	}, nil
}

func (i *IpamImpl) StaticIP() net.IP {
	// Used for the control plane static pod.
	return i.staticIp
}

func (i *IpamImpl) StaticNetwork() NetworkSpec {
	for _, net := range i.staticNets {
		return *net
	}
	return NetworkSpec{}
}

func (i *IpamImpl) AllocateNetwork(ctx context.Context, config *runtimeapi.PodSandboxConfig) (*NetworkSpec, error) {
	i.log.Info().Str("pod", config.Metadata.Name).Msg("allocating network for pod")

	if config.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", config.Metadata.Name).Msg("skipping network allocation for static pod")
		for _, net := range i.staticNets {
			return net, nil
		}
		return nil, fmt.Errorf("no static network available")
	}

	i.staticNetsMu.Lock()
	defer i.staticNetsMu.Unlock()

	var unallocated []NetworkSpec
	var last *NetworkSpec = &i.reservedNet
	for _, net := range i.staticNets {
		if net == nil {
			unallocated = append(unallocated, i.reservedNet)
		} else {
			last = net
		}
	}

	if len(unallocated) != 0 {
		last = &unallocated[0]
	}

	// Calculate the next /30 subnet after the last one.
	var nextNet *net.IPNet
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

	netSpec, err := i.driver.Networks().CreateNetwork(ctx, config.Metadata.Uid, nextNet)
	if err != nil {
		return nil, fmt.Errorf("create network for static pod: %w", err)
	}

	i.staticNets[&nextNet.IP] = &netSpec
	return &netSpec, nil
}

func (i *IpamImpl) DeallocateNetwork(ctx context.Context, status *runtimeapi.PodSandboxStatus) error {
	i.log.Info().Str("pod", status.Metadata.Name).Msg("deallocating network for pod")

	if status.GetAnnotations()["kubernetes.io/config.source"] == "file" {
		i.log.Info().Str("pod", status.Metadata.Name).Msg("skipping network deallocation for static pod")
		return nil
	}

	i.staticNetsMu.Lock()
	defer i.staticNetsMu.Unlock()

	err := i.driver.Networks().RemoveNetwork(ctx, status.Metadata.Uid)
	if err != nil {
		i.log.Warn().Err(err).Str("sandbox", status.Metadata.Name).Msg("failed to remove sandbox network")
	}

	// Remove the network from the staticNets map.
	for ip, net := range i.staticNets {
		if net.Name == status.Metadata.Uid {
			delete(i.staticNets, ip)
			break
		}
	}

	return nil
}

var _ Ipam = &IpamImpl{}
