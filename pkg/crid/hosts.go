package crid

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type FallbackDial func() (net.Conn, error)

var DefaultHosts = &hostsImpl{
	log:      component.NewLogger("hosts"),
	hostname: nil,
	backends: map[backend.Runtime]backend.Backend{},
	addrs:    make(map[string][]string),
}

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to get hostname")
		return
	}
	DefaultHosts.hostname = &hostname

	outboundIps, err := localOutboundIPs()
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to get local outbound IP")
	}

	lookupIps, err := lookupIPs(hostname)
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to lookup IPs for hostname")
	}

	interfaceIps, err := interfaceIPs()
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to get interface IPs")
	}

	DefaultHosts.WithHost(hostname, outboundIps).Log().Info().Str("hostname", hostname).Strs("outboundIPs", outboundIps).Strs("lookupIPs", lookupIps).Strs("interfaceIPs", interfaceIps).Msg("resolved local IP addresses")

	// Sniff test to make sure hostname resolution is working: Check if outbound IPs are included in lookup IPs
	found := false
	for _, outboundIp := range outboundIps {
		for _, lookupIp := range lookupIps {
			if outboundIp == lookupIp {
				found = true
				break
			}
		}
	}

	if !found {
		// panic(fmt.Sprintf("hostname resolution is not working: outbound IPs %v are not included in lookup IPs %v", outboundIps, lookupIps))
	}
}

// localOutboundIP discovers the preferred outbound IP by opening a UDP
// connection to a public address. No packets are actually sent.
func localOutboundIPs() ([]string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return []string{conn.LocalAddr().(*net.UDPAddr).IP.String()}, nil
}

func lookupIPs(host string) ([]string, error) {
	ips, err := net.LookupHost(host)
	if err != nil {
		return nil, err
	}
	return ips, nil
}

func interfaceIPs() ([]string, error) {
	var ips []string
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagRunning == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		ifAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range ifAddrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			if ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() {
				ips = append(ips, ip.String())
			}
		}
	}
	return ips, nil
}

type hostsImpl struct {
	ctx      context.Context
	log      component.Logger
	hostname *string
	backends map[backend.Runtime]backend.Backend
	addrs    map[string][]string
	mu       sync.Mutex

	localAddr     string
	localAddrOnce sync.Once
}

func NewHosts(ctx context.Context, backends map[backend.Runtime]backend.Backend) (backend.Hosts, error) {
	if DefaultHosts.Hostname() == "" {
		return nil, fmt.Errorf("unable to determine hostname")
	}
	for runtime, backend := range backends {
		DefaultHosts.WithBackend(runtime, backend)
	}
	return DefaultHosts.WithContext(ctx), nil
}

func (h *hostsImpl) Log() component.Logger {
	return h.log
}

func (h *hostsImpl) Hostname() string {
	if h.hostname == nil {
		return ""
	}
	return strings.ToLower(*h.hostname)
}

func (h *hostsImpl) WithContext(ctx context.Context) backend.Hosts {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctx = ctx
	return h
}

func (h *hostsImpl) WithHost(name string, addrs []string) backend.Hosts {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hostname = &name
	h.addrs[name] = addrs
	return h
}

func (h *hostsImpl) WithBackend(runtime backend.Runtime, backend backend.Backend) backend.Hosts {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.backends[runtime] = backend
	return h
}

func (h *hostsImpl) Entries(ctx context.Context, network backend.NetworkType) map[string][]string {
	entries := make(map[string][]string)

	for _, ips := range h.addrs {
		for _, ip := range ips {
			if network == backend.NetworkBridge && (strings.HasPrefix(ip, "169.254.") || strings.HasPrefix(ip, "fe80::")) {
				continue
			}
			entries[strings.ToLower(h.Hostname())] = append(entries[strings.ToLower(h.Hostname())], ip)
			for _, backend := range h.backends {
				entries[strings.ToLower(backend.HostnameOverride())] = append(entries[strings.ToLower(backend.HostnameOverride())], ip)
			}
		}
	}

	if network == backend.NetworkHost {
		// TODO: The below it too slow for DNS, short circuit
		return entries
	}

	for _, b := range h.backends {
		info, err := b.HostInfo()
		if err != nil {
			h.log.Error().Err(err).Msg("failed to get host info from backend")
			continue
		}
		domain := info.Domain

		sandboxes, err := b.Containers().ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{})
		if err != nil {
			h.log.Error().Err(err).Msg("failed to list pod sandboxes from backend")
			continue
		}
		for _, sandbox := range sandboxes {
			status, err := b.Containers().PodSandboxStatus(ctx, sandbox.GetId(), false)
			if err != nil {
				h.log.Error().Err(err).Msg("failed to get pod sandbox status from backend")
				continue
			}
			ip := status.GetStatus().GetNetwork().GetIp()
			ips := status.GetStatus().GetNetwork().GetAdditionalIps()
			if ip == "" {
				h.log.Warn().Str("sandbox", sandbox.GetId()).Msg("pod sandbox has no IP address, skipping")
				continue
			}
			h.log.Info().Str("sandbox", sandbox.GetId()).Str("ip", ip).Int("additionalIps", len(ips)).Msg("found pod sandbox IP from backend")
			name := sandbox.GetMetadata().GetName()
			hostname := strings.ToLower(fmt.Sprintf("%s.%s", name, domain))
			entries[hostname] = append(entries[hostname], ip)

			containers, err := b.Containers().ListContainers(ctx, &runtimeapi.ContainerFilter{PodSandboxId: sandbox.GetId()})
			if err != nil {
				h.log.Error().Err(err).Msg("failed to list containers for sandbox from backend")
				continue
			}
			for _, container := range containers {
				h.log.Info().Str("container", container.GetId()).Str("name", container.GetMetadata().GetName()).Str("podSandbox", sandbox.GetId()).Msg("found container in sandbox")
				hostnames := []string{
					strings.ToLower(fmt.Sprintf("%s.%s", container.Metadata.Name, sandbox.Metadata.Namespace)),
					strings.ToLower(fmt.Sprintf("%s.%s.%s", container.Metadata.Name, sandbox.Metadata.Namespace, domain)),
				}
				for _, ip := range ips {
					for _, hostname := range hostnames {
						entries[hostname] = append(entries[hostname], ip.GetIp())
					}
				}
			}
		}
	}

	h.log.Info().Int("count", len(entries)).Msg("Host Entries")
	return entries
}

func (h *hostsImpl) ExtraHosts(ctx context.Context, network backend.NetworkType) []string {
	var extraHosts []string
	entries := h.Entries(ctx, network)
	for hostname, ips := range entries {
		for _, ip := range ips {
			extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", hostname, ip))
		}
	}
	return extraHosts
}

func (h *hostsImpl) HostAliases(ctx context.Context, network backend.NetworkType) []v1.HostAlias {
	ipHosts := make(map[string][]string)
	entries := h.Entries(ctx, network)
	for hostname, ips := range entries {
		for _, ip := range ips {
			ipHosts[ip] = append(ipHosts[ip], strings.ToLower(hostname))
		}
	}

	var hostAliases []v1.HostAlias
	for ip, hostnames := range ipHosts {
		hostAliases = append(hostAliases, v1.HostAlias{
			IP:        ip,
			Hostnames: hostnames,
		})
	}
	return hostAliases
}
