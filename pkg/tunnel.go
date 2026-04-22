package pkg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	httpTimeout        = 15 * time.Second
	gracePeriod        = 30 * time.Second
	serviceUrl         = "https://api.trycloudflare.com"
	cloudflaredVersion = "2026.3.0" // TODO(experimental): mock cloudflared release version reported to the edge; keep aligned with upstream
)

// promMu serializes swaps of prometheus.DefaultRegisterer across all QuickTunnel instances
// so cloudflared's per-supervisor metric registrations can be black-holed without racing.
var promMu sync.Mutex

type TunnelImpl struct {
	ctx         context.Context
	config      v1.Config
	serviceName v1.ServiceName

	localHost     net.IP
	localHostOnce sync.Once

	localPort     int32
	localPortOnce sync.Once

	fqdn      string
	fqdnReady chan struct{}
	fqdnOnce  sync.Once

	// TODO(future): abstract tunnel provider behind an interface
	tunnel      *QuickTunnel
	tunnelReady chan struct{}
}

var _ v1.Tunnel = &TunnelImpl{}

func NewTunnel(config v1.Config, serviceName v1.ServiceName) v1.Tunnel {
	return &TunnelImpl{
		ctx:         config.Context(),
		config:      config,
		serviceName: serviceName,
		fqdnReady:   make(chan struct{}),
		tunnelReady: make(chan struct{}),
	}
}

func (t *TunnelImpl) Context() context.Context {
	return t.ctx
}

func (t *TunnelImpl) LocalIP() net.IP {
	t.localHostOnce.Do(func() {
		conn, _ := net.Dial("udp", "1.1.1.1:53")
		defer conn.Close()
		t.localHost = conn.LocalAddr().(*net.UDPAddr).IP
	})
	return t.localHost
}

func (t *TunnelImpl) LocalPort() int32 {
	t.localPortOnce.Do(func() {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.config.Cancel(nanokube.NewError(fmt.Errorf("failed to acquire tunnel port: %w", err)))
			return
		}
		defer listener.Close()
		t.localPort = int32(listener.Addr().(*net.TCPAddr).Port)
	})
	return t.localPort
}

func (t *TunnelImpl) LocalHostname() string {
	hostname, _ := os.Hostname()
	hostname, _, _ = strings.Cut(hostname, ".")
	return hostname
}

func (t *TunnelImpl) LocalDomain() string {
	hostname, _ := os.Hostname()
	_, domain, _ := strings.Cut(hostname, ".")
	return domain
}

func (t *TunnelImpl) LocalFQDN() string {
	hostname, _ := os.Hostname()
	return hostname
}

func (t *TunnelImpl) FQDN() string {
	t.fqdnOnce.Do(func() {
		// Close both signal channels on any early exit so Ready() consumers
		// never hang when tunnel init fails.
		success := false
		defer func() {
			if !success {
				close(t.fqdnReady)
				close(t.tunnelReady)
			}
		}()

		tunnel, err := newQuickTunnel(t, t.serviceName)
		if err != nil {
			t.config.Cancel(nanokube.NewError(fmt.Errorf("failed to create tunnel: %w", err)))
			return
		}

		t.tunnel = tunnel
		t.fqdn = tunnel.Hostname
		close(t.fqdnReady)
		success = true

		go func() {
			// Unblock Ready() consumers on either successful connect OR stop,
			// so a QuickTunnel that stops before ever connecting doesn't hang callers.
			select {
			case <-t.tunnel.Ready():
				close(t.tunnelReady)
				<-t.tunnel.Stopped()
			case <-t.tunnel.Stopped():
				close(t.tunnelReady)
			}
			t.config.Cancel(nanokube.NewError(fmt.Errorf("tunnel cancelled")))
		}()
	})
	return t.fqdn
}

func (t *TunnelImpl) Hostname() string {
	host, _, _ := strings.Cut(t.FQDN(), ".")
	return host
}

func (t *TunnelImpl) Domain() string {
	_, domain, _ := strings.Cut(t.FQDN(), ".")
	return domain
}

func (t *TunnelImpl) URL() *url.URL {
	<-t.Ready() // ensure FQDN is ready before constructing URL
	return &url.URL{
		Scheme: "https",
		Host:   t.FQDN(),
		Path:   "/",
	}
}

func (t *TunnelImpl) Ready() <-chan struct{} {
	<-t.fqdnReady
	return t.tunnelReady
}

type QuickTunnel struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Hostname   string `json:"hostname"`
	AccountTag string `json:"account_tag"`
	Secret     []byte `json:"secret"`

	ctx    context.Context
	cancel context.CancelFunc
	tunnel v1.Tunnel

	log          *zerolog.Logger
	transportLog *zerolog.Logger

	tunnelConfig     *supervisor.TunnelConfig
	tunnelConfigOnce sync.Once

	orchestrationConfig     *orchestration.Config
	orchestrationConfigOnce sync.Once

	originDialer     *ingress.OriginDialerService
	originDialerOnce sync.Once

	ready       chan struct{}
	stopped     chan struct{}
	connected   *signal.Signal
	reconnected chan supervisor.ReconnectSignal
}

type quickTunnelError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type quickTunnelResponse struct {
	Success bool               `json:"success"`
	Result  QuickTunnel        `json:"result"`
	Errors  []quickTunnelError `json:"errors"`
}

func newQuickTunnel(tunnel v1.Tunnel, serviceName v1.ServiceName) (*QuickTunnel, error) {
	log := zerolog.New(io.Discard).With().Str("service", string(serviceName)).Str("component", "quicktunnel").Logger()
	client := http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout:   httpTimeout,
			ResponseHeaderTimeout: httpTimeout,
		},
		Timeout: httpTimeout,
	}
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

	var data quickTunnelResponse
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

	ctx, cancel := context.WithCancel(tunnel.Context())

	q := &data.Result
	q.ctx = ctx
	q.cancel = cancel

	q.tunnel = tunnel
	q.log = &log
	q.transportLog = &log
	q.ready = make(chan struct{})
	q.stopped = make(chan struct{})
	q.connected = signal.New(make(chan struct{}))
	q.reconnected = make(chan supervisor.ReconnectSignal, 1)

	supervisor, err := q.Supervisor()
	if err != nil {
		return nil, fmt.Errorf("failed to create supervisor: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- supervisor.Run(q.ctx, q.connected)
	}()

	go func() {
		defer close(q.stopped)
		defer cancel()
		connectedWait := q.connected.Wait()
		for {
			select {
			case <-connectedWait:
				nanokube.Log.Info("tunnel connected")
				close(q.ready)
				connectedWait = nil // disable this case; Signal fires once but its channel stays readable
			case <-q.reconnected:
				nanokube.Log.Info("tunnel reconnecting")
			case err := <-done:
				if err != nil {
					nanokube.Log.Error("tunnel exited with error")
				} else {
					nanokube.Log.Debug("tunnel exited")
				}
				return
			}
		}
	}()

	return q, nil
}

func (q *QuickTunnel) Ready() <-chan struct{} {
	return q.ready
}

func (q *QuickTunnel) Stopped() <-chan struct{} {
	return q.stopped
}

func (q *QuickTunnel) Supervisor() (*supervisor.Supervisor, error) {
	promMu.Lock()
	defer promMu.Unlock()

	registerer := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = noop()
	defer func() { prometheus.DefaultRegisterer = registerer }()

	internalRules := []ingress.Rule{}
	orchestrator, err := orchestration.NewOrchestrator(q.ctx, q.OrchestrationConfig(), q.TunnelConfig().Tags, internalRules, q.log)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator: %w", err)
	}

	supervisor, err := supervisor.NewSupervisor(q.TunnelConfig(), orchestrator, q.reconnected, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create supervisor: %w", err)
	}

	return supervisor, nil
}

func (q *QuickTunnel) TunnelConfig() *supervisor.TunnelConfig {
	q.tunnelConfigOnce.Do(func() {
		tunnelConfig := &supervisor.TunnelConfig{
			ClientConfig: func() *client.Config {
				featureSelector, _ := features.NewFeatureSelector(q.ctx, q.AccountTag, nil, false, q.log)
				cfg, _ := client.NewConfig(cloudflaredVersion, fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH), featureSelector)
				return cfg
			}(),
			GracePeriod:     gracePeriod,
			Region:          "",
			EdgeIPVersion:   allregions.Auto,
			HAConnections:   1,                                     // Quick tunnels use single connection
			Tags:            []pogs.Tag{{Name: "ID", Value: q.ID}}, // TODO(experimental): reuse tunnel ID as connector ID; cloudflared normally generates a fresh UUID per process
			Log:             q.log,
			LogTransport:    q.transportLog,
			Observer:        connection.NewObserver(q.log, q.log),
			ReportedVersion: cloudflaredVersion,
			Retries:         5,
			RunFromTerminal: false,
			NamedTunnel: func() *connection.TunnelProperties {
				tunnelID, _ := uuid.Parse(q.ID)
				return &connection.TunnelProperties{
					Credentials: connection.Credentials{
						AccountTag:   q.AccountTag,
						TunnelSecret: q.Secret,
						TunnelID:     tunnelID,
					},
					QuickTunnelUrl: q.Hostname,
				}
			}(),
			ProtocolSelector: func() connection.ProtocolSelector {
				protocolSelector, _ := connection.NewProtocolSelector("auto", q.AccountTag, false, false, edgediscovery.ProtocolPercentage, connection.ResolveTTL, q.log)
				return protocolSelector
			}(),
			EdgeTLSConfigs: func() map[connection.Protocol]*tls.Config {
				rootCAs, _ := x509.SystemCertPool()
				cfCAs, _ := tlsconfig.GetCloudflareRootCA()
				for _, c := range cfCAs {
					rootCAs.AddCert(c)
				}
				out := make(map[connection.Protocol]*tls.Config, len(connection.ProtocolList))
				for _, p := range connection.ProtocolList {
					s := p.TLSSettings()
					out[p] = &tls.Config{ServerName: s.ServerName, NextProtos: s.NextProtos, RootCAs: rootCAs}
				}
				return out
			}(),
			MaxEdgeAddrRetries:  8,
			RPCTimeout:          5 * time.Second,
			OriginDNSService:    origins.NewDNSResolverService(q.OriginDialer(), q.log, noop()),
			OriginDialerService: q.OriginDialer(),
		}
		q.tunnelConfig = tunnelConfig
	})
	return q.tunnelConfig
}

func (q *QuickTunnel) OriginDialer() *ingress.OriginDialerService {
	q.originDialerOnce.Do(func() {
		originDialer := ingress.NewOriginDialer(ingress.OriginConfig{}, q.log) // DefaultDialer overwritten by orchestrator; TCPWriteTimeout default 0 matches cloudflared
		q.originDialer = originDialer
	})
	return q.originDialer
}

func (q *QuickTunnel) OrchestrationConfig() *orchestration.Config {
	noTLSVerify := true // kube-apiserver presents a self-signed cert
	http2Origin := v1.HTTP2

	q.orchestrationConfigOnce.Do(func() {
		orchestrationConfig := &orchestration.Config{
			Ingress: func() *ingress.Ingress {
				parsed, _ := ingress.ParseIngress(&config.Configuration{
					OriginRequest: config.OriginRequestConfig{
						NoTLSVerify: &noTLSVerify,
						Http2Origin: &http2Origin,
					},
					WarpRouting: config.WarpRoutingConfig{},
					Ingress: []config.UnvalidatedIngressRule{
						{Service: fmt.Sprintf("https://%s:%d", q.tunnel.LocalIP(), q.tunnel.LocalPort())},
					},
				})
				return &parsed
			}(),
			WarpRouting:         ingress.NewWarpRoutingConfig(&config.WarpRoutingConfig{}), // cloudflared defaults: 5s connect, unlimited flows, 30s keepalive
			OriginDialerService: q.OriginDialer(),
			ConfigurationFlags:  map[string]string{}, // CLI-flag overrides for remote config; empty matches cloudflared quick-tunnel behavior
		}
		q.orchestrationConfig = orchestrationConfig
	})
	return q.orchestrationConfig
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
