package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/breml/rootcerts/embedded"
	"github.com/cloudflare/cloudflared/client"
	"github.com/cloudflare/cloudflared/config"
	"github.com/cloudflare/cloudflared/connection"
	"github.com/cloudflare/cloudflared/edgediscovery"
	"github.com/cloudflare/cloudflared/edgediscovery/allregions"
	"github.com/cloudflare/cloudflared/features"
	"github.com/cloudflare/cloudflared/ingress"
	"github.com/cloudflare/cloudflared/ingress/origins"
	"github.com/cloudflare/cloudflared/orchestration"
	"github.com/cloudflare/cloudflared/signal"
	"github.com/cloudflare/cloudflared/supervisor"
	"github.com/cloudflare/cloudflared/tlsconfig"
	"github.com/cloudflare/cloudflared/tunnelrpc/pogs"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"k8s.io/klog/v2"
)

type Tunnel interface {
	LocalIP() net.IP
	LocalPort() int
	LocalHost() string
	LocalURL() *url.URL

	Host() string
	Hostname() string
	Domain() string
	URL() *url.URL
	CACerts() []*x509.Certificate

	WithListener(listener net.Listener) Tunnel
	Listening() <-chan struct{}
	HostnameReady() <-chan struct{}
}

type TunnelImpl struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	log *zerolog.Logger

	localHostOnce sync.Once
	__localHost__ string

	localIPOnce sync.Once
	__localIP__ net.IP

	localPortOnce sync.Once
	__localPort__ int

	localURLOnce sync.Once
	__localURL__ *url.URL

	specOnce sync.Once
	__spec__ *spec

	hostnameOnce sync.Once
	__hostname__ string

	caCertsOnce sync.Once
	__caCerts__ []*x509.Certificate

	listeningOnce sync.Once
	__listening__ chan struct{}

	hostnameReadyOnce sync.Once
	__hostnameReady__ chan struct{}

	urlOnce sync.Once
	__url__ *url.URL
}

type spec struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Hostname   string `json:"hostname"`
	AccountTag string `json:"account_tag"`
	Secret     []byte `json:"secret"`
}

type noopImpl struct {
	origins.Metrics
	prometheus.Registerer
}

func noop() *noopImpl {
	return &noopImpl{}
}

func (n *noopImpl) IncrementDNSTCPRequests() {}
func (n *noopImpl) IncrementDNSUDPRequests() {}

func (n *noopImpl) Register(prometheus.Collector) error  { return nil }
func (n *noopImpl) MustRegister(...prometheus.Collector) {}
func (n *noopImpl) Unregister(prometheus.Collector) bool { return true }

var (
	_                  Tunnel = &TunnelImpl{}
	promMu             sync.Mutex
	cloudflaredVersion = "2026.3.0"
)

// klogWriter adapts zerolog's io.Writer sink onto klog so cloudflared tunnel
// logs surface through the normal nanokube log stream instead of being
// discarded. zerolog accepts any io.Writer; klog has no matching ingress
// writer, so forward each emitted line as a klog record.
type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(4).Info(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func NewTunnel() Tunnel {
	ctx, cancel := context.WithCancelCause(context.Background())
	log := zerolog.New(klogWriter{}).With().Str("component", "tunnel").Logger()

	t := &TunnelImpl{
		ctx:               ctx,
		cancel:            cancel,
		log:               &log,
		__listening__:     make(chan struct{}),
		__hostnameReady__: make(chan struct{}),
	}

	// If a resolved spec was inherited via the environment (e.g. across the
	// daemonize re-exec), adopt it and pre-fire specOnce/hostnameOnce so this
	// tunnel reuses the exact same hostname/credentials instead of resolving a
	// second trycloudflare tunnel.
	if env := os.Getenv("TUNNEL_SPEC"); env != "" {
		var s spec
		if err := json.Unmarshal([]byte(env), &s); err != nil {
			log.Error().Err(err).Msg("unable to parse TUNNEL_SPEC from environment")
		} else {
			t.specOnce.Do(func() { t.__spec__ = &s })
			t.hostnameOnce.Do(func() {
				t.__hostname__ = s.Hostname
				os.Setenv("TUNNEL_HOSTNAME", t.__hostname__)
			})
			log.Info().Str("hostname", s.Hostname).Msg("adopted tunnel spec from environment")
		}
	}

	// Surface why the tunnel context was canceled. cancel is a
	// CancelCauseFunc, so every t.cancel(err) records a cause that
	// context.Cause reports here when Done fires.
	go func() {
		<-ctx.Done()
		t.log.Warn().Err(context.Cause(ctx)).Msg("tunnel context canceled")
	}()

	return t
}

func (t *TunnelImpl) LocalIP() net.IP {
	t.localIPOnce.Do(func() {
		t.log.Info().Msg("determining local IP for tunnel")
		conn, err := net.Dial("udp", "1.1.1.1:53")
		if err != nil {
			t.cancel(fmt.Errorf("unable to get local IP: %w", err))
			return
		}
		defer conn.Close()
		t.__localIP__ = conn.LocalAddr().(*net.UDPAddr).IP
		t.log.Info().Str("localIP", t.__localIP__.String()).Msg("determined local IP for tunnel")
	})
	return t.__localIP__
}

func (t *TunnelImpl) LocalPort() int {
	t.localPortOnce.Do(func() {
		t.log.Info().Msg("acquiring local port for tunnel")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.cancel(fmt.Errorf("unable to acquire local port: %w", err))
			return
		}
		defer listener.Close()
		t.__localPort__ = int(listener.Addr().(*net.TCPAddr).Port)
		t.log.Info().Int("localPort", t.__localPort__).Msg("acquired local port for tunnel")
	})
	return t.__localPort__
}

func (t *TunnelImpl) LocalHost() string {
	t.localHostOnce.Do(func() {
		t.log.Info().Msg("determining local hostname for tunnel")
		hostname, err := os.Hostname()
		if err != nil {
			t.cancel(fmt.Errorf("unable to get local hostname: %w", err))
			return
		}
		hostname, _, _ = strings.Cut(hostname, ".")
		t.__localHost__ = hostname
		t.log.Info().Str("localHost", t.__localHost__).Msg("determined local hostname for tunnel")
	})
	return t.__localHost__
}

func (t *TunnelImpl) LocalURL() *url.URL {
	t.localURLOnce.Do(func() {
		t.__localURL__ = &url.URL{
			Scheme: "https",
			Host:   net.JoinHostPort(t.LocalIP().String(), strconv.Itoa(t.LocalPort())),
			// Path:   "/", // TODO(partial): etcd hates the path component
		}
	})
	return t.__localURL__
}

func (t *TunnelImpl) Hostname() string {
	t.hostnameOnce.Do(func() {
		t.__hostname__ = t.spec().Hostname
		os.Setenv("TUNNEL_HOSTNAME", t.__hostname__)
	})
	return t.__hostname__
}

func (t *TunnelImpl) Host() string {
	hostname := t.Hostname()
	host, _, _ := strings.Cut(hostname, ".")
	return host
}

func (t *TunnelImpl) Domain() string {
	hostname := t.Hostname()
	_, domain, _ := strings.Cut(hostname, ".")
	return domain
}

func (t *TunnelImpl) URL() *url.URL {
	t.urlOnce.Do(func() {
		hostname := t.Hostname()
		<-await(t.ctx, t.HostnameReady())
		t.__url__ = &url.URL{
			Scheme: "https",
			Host:   hostname + ":443",
			Path:   "/",
		}
	})
	return t.__url__
}

func (t *TunnelImpl) CACerts() []*x509.Certificate {
	t.caCertsOnce.Do(func() {
		t.log.Info().Msg("loading CA certificates for tunnel")
		certificates := []*x509.Certificate{}

		rest := []byte(embedded.MozillaCACertificatesPEM())
		for {
			block, remainder := pem.Decode(rest)
			if block == nil {
				break
			}
			rest = remainder
			if block.Type != "CERTIFICATE" {
				continue
			}
			if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
				certificates = append(certificates, cert)
			}
		}

		cloudflare, _ := tlsconfig.GetCloudflareRootCA()
		certificates = append(certificates, cloudflare...)

		t.__caCerts__ = certificates
		t.log.Info().Int("numCACerts", len(t.__caCerts__)).Msg("loaded CA certificates for tunnel")
	})
	return t.__caCerts__
}

func (t *TunnelImpl) WithListener(listener net.Listener) Tunnel {
	t.listeningOnce.Do(func() {
		t.log.Info().Str("address", listener.Addr().String()).Msg("configuring tunnel with local listener")
		if listener.Addr().(*net.TCPAddr).Port != t.LocalPort() {
			t.cancel(fmt.Errorf("listener port (%d) does not match local port (%d)", listener.Addr().(*net.TCPAddr).Port, t.LocalPort()))
			return
		}

		go func() {
			t.log.Info().Msg("starting tunnel with local listener")
			promMu.Lock()
			defer promMu.Unlock()

			registerer := prometheus.DefaultRegisterer
			prometheus.DefaultRegisterer = noop()
			defer func() { prometheus.DefaultRegisterer = registerer }()

			originDialer := ingress.NewOriginDialer(ingress.OriginConfig{}, t.log)

			tunnelConfig := &supervisor.TunnelConfig{
				ClientConfig: func() *client.Config {
					featureSelector, _ := features.NewFeatureSelector(t.ctx, t.spec().AccountTag, nil, false, t.log)
					cfg, _ := client.NewConfig(cloudflaredVersion, fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH), featureSelector)
					return cfg
				}(),
				GracePeriod:     60 * time.Second, // TODO(partial): what is a good default here?
				Region:          "",
				EdgeIPVersion:   allregions.Auto,
				HAConnections:   1,
				Tags:            []pogs.Tag{{Name: "ID", Value: t.spec().ID}}, // TODO(experimental): reuse tunnel ID as connector ID; cloudflared normally generates a fresh UUID per process
				Log:             t.log,
				LogTransport:    t.log,
				Observer:        connection.NewObserver(t.log, t.log),
				ReportedVersion: cloudflaredVersion,
				Retries:         5,
				RunFromTerminal: false,
				NamedTunnel: func() *connection.TunnelProperties {
					tunnelID, _ := uuid.Parse(t.spec().ID)
					return &connection.TunnelProperties{
						Credentials: connection.Credentials{
							AccountTag:   t.spec().AccountTag,
							TunnelSecret: t.spec().Secret,
							TunnelID:     tunnelID,
						},
						QuickTunnelUrl: t.Hostname(),
					}
				}(),
				ProtocolSelector: func() connection.ProtocolSelector {
					protocolSelector, _ := connection.NewProtocolSelector("auto", t.spec().AccountTag, false, false, edgediscovery.ProtocolPercentage, connection.ResolveTTL, t.log)
					return protocolSelector
				}(),
				EdgeTLSConfigs: func() map[connection.Protocol]*tls.Config {
					pool := x509.NewCertPool()
					for _, c := range t.CACerts() {
						pool.AddCert(c)
					}
					out := make(map[connection.Protocol]*tls.Config, len(connection.ProtocolList))
					for _, p := range connection.ProtocolList {
						s := p.TLSSettings()
						out[p] = &tls.Config{ServerName: s.ServerName, NextProtos: s.NextProtos, RootCAs: pool}
					}
					return out
				}(),
				MaxEdgeAddrRetries:  8,
				RPCTimeout:          5 * time.Second,
				OriginDNSService:    origins.NewDNSResolverService(originDialer, t.log, noop()),
				OriginDialerService: originDialer,
			}

			noTLSVerify := true
			http2Origin := true
			internalRules := []ingress.Rule{}
			orchestrator, err := orchestration.NewOrchestrator(t.ctx, &orchestration.Config{
				Ingress: func() *ingress.Ingress {
					parsed, _ := ingress.ParseIngress(&config.Configuration{
						OriginRequest: config.OriginRequestConfig{
							NoTLSVerify: &noTLSVerify,
							Http2Origin: &http2Origin,
						},
						WarpRouting: config.WarpRoutingConfig{},
						Ingress: []config.UnvalidatedIngressRule{
							{Service: fmt.Sprintf("https://%s:%d", listener.Addr().(*net.TCPAddr).IP.String(), listener.Addr().(*net.TCPAddr).Port)},
						},
					})
					return &parsed
				}(),
				WarpRouting:         ingress.NewWarpRoutingConfig(&config.WarpRoutingConfig{}), // cloudflared defaults: 5s connect, unlimited flows, 30s keepalive
				OriginDialerService: originDialer,
				ConfigurationFlags:  map[string]string{}, // CLI-flag overrides for remote config; empty matches cloudflared quick-tunnel behavior
			}, tunnelConfig.Tags, internalRules, t.log)
			if err != nil {
				t.cancel(fmt.Errorf("failed to create orchestrator: %w", err))
			}

			connected := signal.New(make(chan struct{}))
			reconnected := make(chan supervisor.ReconnectSignal) // TODO(partial): what to do with reconnected
			supervisor, err := supervisor.NewSupervisor(tunnelConfig, orchestrator, reconnected, t.ctx.Done())
			if err != nil {
				t.cancel(fmt.Errorf("failed to create supervisor: %w", err))
			}

			if err := supervisor.Run(t.ctx, connected); err != nil {
				t.cancel(fmt.Errorf("supervisor run failed: %w", err))
			}

			t.log.Info().Msg("waiting for tunnel to be connected")
			<-connected.Wait()
			t.log.Info().Msg("tunnel connected, waiting for DNS")
			<-t.HostnameReady()
			t.log.Info().Msg("tunnel is listening")
			close(t.__listening__)
		}()
	})
	return t
}

func (t *TunnelImpl) HostnameReady() <-chan struct{} {
	t.hostnameReadyOnce.Do(func() {
		// authServers resolves the zone's authoritative nameservers to ip:53
		// endpoints. Run once up front and stashed; NS records are stable.
		authServers := func() []string {
			ns, err := net.DefaultResolver.LookupNS(t.ctx, t.Domain())
			if err != nil {
				return nil
			}

			ips := make([]string, 0, len(ns))
			for _, record := range ns {
				hostIPs, err := net.DefaultResolver.LookupHost(t.ctx, record.Host)
				if err != nil {
					continue
				}
				for _, ip := range hostIPs {
					ips = append(ips, net.JoinHostPort(ip, "53"))
				}
			}
			return ips
		}

		go func() {
			// Discover the authoritative servers once, retrying until available.
			var ips []string
			for {
				if ips = authServers(); len(ips) > 0 {
					break
				}
				<-await(t.ctx, time.After(time.Second))
			}

			d := net.Dialer{Timeout: 5 * time.Second}
			r := &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
					// randomize across all authoritative servers instead of always picking the first one
					return d.DialContext(ctx, network, ips[time.Now().UnixNano()%int64(len(ips))])
				},
			}

			for {
				ctx, cancel := context.WithTimeout(t.ctx, 5*time.Second)
				addrs, err := r.LookupHost(ctx, t.Hostname())
				cancel()

				if err == nil && len(addrs) > 0 {
					t.log.Info().Str("hostname", t.Hostname()).Strs("addrs", addrs).Msg("hostname resolved on authoritative nameservers")
					close(t.__hostnameReady__)
					return
				}

				<-await(t.ctx, time.After(time.Second))
			}
		}()
	})
	return t.__hostnameReady__
}

func (t *TunnelImpl) Listening() <-chan struct{} {
	return t.__listening__
}

func (t *TunnelImpl) spec() *spec {
	t.specOnce.Do(func() {
		t.log.Info().Msg("fetching tunnel spec")
		if env := os.Getenv("TUNNEL_SPEC"); env != "" {
			var spec spec
			if err := json.Unmarshal([]byte(env), &spec); err != nil {
				t.cancel(fmt.Errorf("unable to parse TUNNEL_SPEC: %w", err))
				return
			}
			t.__spec__ = &spec

			t.log.Info().Any("spec", spec).Msg("fetched tunnel spec from environment variable")
			return
		}

		client := http.Client{
			Transport: &http.Transport{
				TLSHandshakeTimeout:   15 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
			},
			Timeout: 15 * time.Second,
		}

		fetch := func() (*spec, error) {
			req, err := http.NewRequest(http.MethodPost, "https://api.trycloudflare.com/tunnel", nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("User-Agent", fmt.Sprintf("cloudflared/%s", cloudflaredVersion))

			resp, err := client.Do(req)
			if err != nil {
				return nil, fmt.Errorf("failed to request tunnel credentials: %w", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read tunnel credentials response: %w", err)
			}

			if resp.StatusCode == http.StatusTooManyRequests {
				retryAfter := resp.Header.Get("Retry-After")
				if secs, err := strconv.Atoi(retryAfter); err == nil {
					now := time.Now()
					return nil, fmt.Errorf("tunnel rate limit resets in %s", humanize.RelTime(now.Add(time.Duration(secs)*time.Second), now, "", ""))
				}
				if retryAfter != "" {
					return nil, fmt.Errorf("tunnel rate limit hit (HTTP 429): Retry-After=%s", retryAfter)
				}
				return nil, fmt.Errorf("tunnel rate limit hit (HTTP 429): no rate-limit headers returned")
			}

			type response struct {
				Success bool `json:"success"`
				Errors  []struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"errors"`
				Result spec `json:"result"`
			}

			var data response
			if err := json.Unmarshal(body, &data); err != nil {
				return nil, fmt.Errorf("tunnel credentials request failed (status=%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}

			if !data.Success {
				var errorMessages []string
				for _, e := range data.Errors {
					errorMessages = append(errorMessages, fmt.Sprintf("%d: %s", e.Code, e.Message))
				}
				return nil, fmt.Errorf("tunnel credential request failed: %s", strings.Join(errorMessages, "; "))
			}
			return &data.Result, nil
		}

		sleep := 0 * time.Second
		for {
			spec, err := fetch()
			if err == nil {
				if bytes, err := json.Marshal(spec); err == nil {
					os.Setenv("TUNNEL_SPEC", string(bytes))
				}
				t.__spec__ = spec
				t.log.Info().Any("spec", spec).Msg("fetched tunnel spec from API")
				return
			}
			t.log.Warn().Err(err).Msg("failed to fetch tunnel spec, retrying...")

			sleep += 1 * time.Second
			<-await(t.ctx, time.After(sleep))
		}
	})
	return t.__spec__
}

func await[T any](ctx context.Context, ch <-chan T) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-ch:
			close(out)
		}
	}()
	return out
}
