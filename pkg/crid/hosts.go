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
	"github.com/miekg/dns"
	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type FallbackDial func() (net.Conn, error)

var DefaultHosts = &hostsImpl{
	log:      component.NewLogger("hosts"),
	hostname: nil,
	backends: map[backend.Runtime]backend.Backend{},
	addrs:    make(map[string][]string),
	resolver: net.DefaultResolver,
}

func init() {
	DefaultHosts.mu.Lock()
	defer DefaultHosts.mu.Unlock()

	hostname, err := os.Hostname()
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to get hostname")
		return
	}
	DefaultHosts.hostname = &hostname

	var ifIps []string = make([]string, 0)
	var nsIps []string = make([]string, 0)

	interfaces, err := net.Interfaces()
	if err != nil {
		DefaultHosts.log.Error().Err(err).Msg("failed to get network interfaces")
		return
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
				DefaultHosts.addrs[hostname] = append(DefaultHosts.addrs[hostname], ip.String())
			}
			ifIps = append(ifIps, ip.String())
		}
	}

	nsIps, err = net.LookupHost(hostname)
	DefaultHosts.log.Info().Str("hostname", hostname).Strs("interfaceIPs", ifIps).Strs("nslookupIPs", nsIps).Msg("resolved local IP addresses")

	if err != nil {
		panic(fmt.Sprintf("failed to resolve hostname '%s' to IP addresses: %v", hostname, err))
	}

	// net.DefaultResolver = DefaultHosts.Resolver()
}

type hostsImpl struct {
	ctx      context.Context
	log      component.Logger
	hostname *string
	backends map[backend.Runtime]backend.Backend
	addrs    map[string][]string
	mu       sync.Mutex

	resolver     *net.Resolver
	resolverOnce sync.Once

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

func (h *hostsImpl) Entries(ctx context.Context, network backend.Network) map[string][]string {
	entries := make(map[string][]string)

	hostname := h.hostname
	if hostname != nil {
		for _, ips := range h.addrs {
			for _, ip := range ips {
				if network == backend.NetworkBridge && (strings.HasPrefix(ip, "169.254.") || strings.HasPrefix(ip, "fe80::")) {
					continue
				}
				entries[strings.ToLower(*hostname)] = append(entries[strings.ToLower(*hostname)], ip)
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
			continue
		}
		domain := info.Domain

		sandboxes, err := b.Containers().ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{})
		if err != nil {
			continue
		}
		for _, sandbox := range sandboxes {
			status, err := b.Containers().PodSandboxStatus(ctx, sandbox.GetId(), false)
			if err != nil {
				continue
			}
			ip := status.GetStatus().GetNetwork().GetIp()
			if ip == "" {
				continue
			}
			name := sandbox.GetMetadata().GetName()
			hostname := strings.ToLower(fmt.Sprintf("%s.%s", name, domain))
			entries[hostname] = append(entries[hostname], ip)
		}
	}

	return entries
}

func (h *hostsImpl) ExtraHosts(ctx context.Context, network backend.Network) []string {
	var extraHosts []string
	entries := h.Entries(ctx, network)
	for hostname, ips := range entries {
		for _, ip := range ips {
			extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", hostname, ip))
		}
	}
	return extraHosts
}

func (h *hostsImpl) HostAliases(ctx context.Context, network backend.Network) []v1.HostAlias {
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

func (h *hostsImpl) Resolver() *net.Resolver {
	h.resolverOnce.Do(func() {
		h.resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				fallback := func() (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}
				return h.dial(ctx, network, address, fallback)
			},
		}
	})
	return h.resolver
}

func (h *hostsImpl) dial(ctx context.Context, _, _ string, fallback FallbackDial) (net.Conn, error) {
	h.mu.Lock()
	appCtx := h.ctx
	h.mu.Unlock()

	if appCtx == nil {
		return fallback()
	}

	localAddr := h.startDNS(appCtx)
	if localAddr == "" {
		return fallback()
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "udp", localAddr)
	if err != nil {
		h.log.Warn().Err(err).Msg("local DNS server unreachable, using fallback")
		return fallback()
	}
	return conn, nil
}

func (h *hostsImpl) startDNS(ctx context.Context) string {
	h.localAddrOnce.Do(func() {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			h.log.Error().Err(err).Msg("failed to start DNS listener")
			return
		}
		h.localAddr = pc.LocalAddr().String()
		h.log.Info().Str("addr", h.localAddr).Msg("DNS server listening")

		go func() {
			<-ctx.Done()
			pc.Close()
		}()

		go func() {
			buf := make([]byte, 4096)
			for {
				n, addr, err := pc.ReadFrom(buf)
				if err != nil {
					return
				}
				go h.lookup(ctx, pc, addr, buf[:n])
			}
		}()
	})
	return h.localAddr
}

func addAnswers(resp *dns.Msg, qName string, qType uint16, ips []string, ttl uint32) {
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if qType == dns.TypeA && ip.To4() != nil {
			resp.Answer = append(resp.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: qName, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
				A:   ip.To4(),
			})
		} else if qType == dns.TypeAAAA && ip.To4() == nil {
			resp.Answer = append(resp.Answer, &dns.AAAA{
				Hdr:  dns.RR_Header{Name: qName, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: ttl},
				AAAA: ip,
			})
		}
	}
}

func (h *hostsImpl) lookup(ctx context.Context, pc net.PacketConn, addr net.Addr, query []byte) {
	msg := new(dns.Msg)
	if err := msg.Unpack(query); err != nil {
		return
	}

	resp := new(dns.Msg)
	resp.SetReply(msg)

	entries := h.Entries(ctx, backend.NetworkHost)
	for _, q := range msg.Question {
		name := strings.ToLower(strings.TrimSuffix(q.Name, "."))
		if ips, found := entries[name]; found {
			addAnswers(resp, q.Name, q.Qtype, ips, 5)
		}
	}

	if len(resp.Answer) > 0 {
		h.log.Debug().Str("name", msg.Question[0].Name).Int("answers", len(resp.Answer)).Msg("resolved from hosts")
		out, _ := resp.Pack()
		pc.WriteTo(out, addr)
		return
	}

	// Fallback: CGO system resolver (handles macOS/Tailscale/mDNS)
	q := msg.Question[0]
	name := strings.TrimSuffix(q.Name, ".")
	sysResolver := &net.Resolver{PreferGo: false}
	ips, err := sysResolver.LookupHost(ctx, name)
	if err != nil {
		h.log.Debug().Err(err).Str("name", name).Msg("system lookup failed")
		resp.Rcode = dns.RcodeNameError
		out, _ := resp.Pack()
		pc.WriteTo(out, addr)
		return
	}

	addAnswers(resp, q.Name, q.Qtype, ips, 30)
	out, _ := resp.Pack()
	pc.WriteTo(out, addr)
}
