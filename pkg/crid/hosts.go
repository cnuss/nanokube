package crid

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/cnuss/nanokube/pkg/crid/backend"
	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type hostsImpl struct {
	ctx      context.Context
	hostname string
	backends map[backend.Runtime]backend.Backend
	addrs    map[string][]string
}

func newHosts(ctx context.Context, backends map[backend.Runtime]backend.Backend) (backend.Hosts, error) {
	addrs := make(map[string][]string)

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("failed to get network interfaces: %w", err)
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
				addrs[hostname] = append(addrs[hostname], ip.String())
			}
		}
	}
	return &hostsImpl{
		ctx:      ctx,
		hostname: hostname,
		backends: backends,
		addrs:    addrs,
	}, nil
}

func (h *hostsImpl) Hostname() string {
	return h.hostname
}

func (h *hostsImpl) ExtraHosts(network backend.Network) []string {
	var extraHosts []string
	for _, ips := range h.addrs {
		for _, ip := range ips {
			if network == backend.NetworkBridge && (strings.HasPrefix(ip, "169.254.") || strings.HasPrefix(ip, "fe80::")) {
				continue
			}
			extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", h.hostname, ip))
		}
	}

	for _, b := range h.backends {
		info, err := b.HostInfo()
		if err != nil {
			continue
		}
		domain := info.Domain

		sandboxes, err := b.Containers().ListPodSandbox(h.ctx, &runtimeapi.PodSandboxFilter{})
		if err != nil {
			continue
		}
		for _, sandbox := range sandboxes {
			status, err := b.Containers().PodSandboxStatus(h.ctx, sandbox.GetId(), false)
			if err != nil {
				continue
			}
			ip := status.GetStatus().GetNetwork().GetIp()
			if ip == "" {
				continue
			}
			name := sandbox.GetMetadata().GetName()
			hostname := fmt.Sprintf("%s.%s", name, domain)
			extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", hostname, ip))
		}
	}

	return extraHosts
}

func (h *hostsImpl) HostAliases(network backend.Network) []v1.HostAlias {
	aliases := make(map[string][]string)
	for _, entry := range h.ExtraHosts(network) {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) == 2 {
			aliases[parts[1]] = append(aliases[parts[1]], strings.ToLower(parts[0]))
		}
	}
	result := make([]v1.HostAlias, 0, len(aliases))
	for ip, hostnames := range aliases {
		result = append(result, v1.HostAlias{IP: ip, Hostnames: hostnames})
	}
	return result
}
