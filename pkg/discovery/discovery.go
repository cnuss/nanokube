package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/tunnel"
	"k8s.io/klog/v2"
)

// userAgent honestly identifies nanokube to kvdb.io.
const (
	userAgent        = "nanokube-discovery (+https://github.com/cnuss/nanokube)"
	discoveryService = "kvdb.io"
)

type Discovery interface {
	Seed() string
	Peers() string

	WithTunnel(tunnel tunnel.Tunnel) Discovery
	Tunnel() tunnel.Tunnel
}

type discoveryImpl struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	seedFile string

	httpOnce sync.Once
	http     *http.Client

	tunnelOnce     sync.Once
	tunnel         tunnel.Tunnel
	tunnelProvided chan struct{}

	seed     string
	seedOnce sync.Once
}

// NewDiscovery builds a Discovery bound to ctx. cancel is the parent's
// cancel-with-cause (e.g. nanokube's CancelErr): discovery failures call it
// directly so cancellation bubbles up to the whole process rather than being
// swallowed by a private child context.
func NewDiscovery(ctx context.Context, cancel context.CancelCauseFunc, seedFile string) Discovery {
	discovery := &discoveryImpl{
		ctx:            ctx,
		cancel:         cancel,
		seedFile:       seedFile,
		tunnelProvided: make(chan struct{}),
	}
	return discovery
}

func (d *discoveryImpl) client() *http.Client {
	d.httpOnce.Do(func() {
		d.http = &http.Client{
			Transport: &http.Transport{
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: 5 * time.Second,
			},
			Timeout: 5 * time.Second,
		}
	})
	return d.http
}

func (d *discoveryImpl) WithTunnel(tunnel tunnel.Tunnel) Discovery {
	d.tunnelOnce.Do(func() {
		klog.Infof("discovery: registering tunnel %s with discovery service", tunnel.Hostname())
		payload := tunnel.LocalHost()
		url := fmt.Sprintf("https://%s/%s/%s", discoveryService, d.Seed(), tunnel.Hostname())

		req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, url, strings.NewReader(payload))
		if err != nil {
			d.cancel(fmt.Errorf("failed to create request: %w", err))
			return
		}
		resp, err := do(d.ctx, d.client(), req)
		if err != nil {
			d.cancel(fmt.Errorf("failed to do request: %w", err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			d.cancel(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
			return
		}
		d.tunnel = tunnel
		close(d.tunnelProvided)
	})
	return d
}

func (d *discoveryImpl) Tunnel() tunnel.Tunnel {
	<-await(d.ctx, d.tunnelProvided)
	return d.tunnel
}

func (d *discoveryImpl) Seed() string {
	d.seedOnce.Do(func() {
		if seed := os.Getenv("NANOKUBE_SEED"); seed != "" {
			d.seed = seed
			return
		}
		if data, err := os.ReadFile(d.seedFile); err == nil {
			d.seed = string(data)
			return
		}

		url := fmt.Sprintf("https://%s", discoveryService)
		// payload := strings.NewReader(fmt.Sprintf("email=%s@nanokube.xyz", discoveryService))
		payload := strings.NewReader(fmt.Sprintf("email=%s@cnuss.com", discoveryService)) // TODO(partial): move from cnuss.com

		req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, url, payload)
		if err != nil {
			d.cancel(fmt.Errorf("failed to create request: %w", err))
			return
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := do(d.ctx, d.client(), req)
		if err != nil {
			d.cancel(fmt.Errorf("failed to do request: %w", err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			d.cancel(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
			return
		}
		location := resp.Header.Get("Location")
		if location == "" {
			d.cancel(fmt.Errorf("missing Location header"))
			return
		}
		seed := strings.Trim(path.Base(location), "/")
		d.seed = seed
		err = os.WriteFile(d.seedFile, []byte(seed), 0o600)
		if err != nil {
			klog.Errorf("discovery: failed to write seed file: %v", err)
			d.cancel(fmt.Errorf("failed to write seed file: %w", err))
			return
		}
		os.Setenv("NANOKUBE_SEED", seed)
		klog.Infof("discovery: obtained seed bucket %s", seed)
	})
	return d.seed
}

func (d *discoveryImpl) Peers() string {
	url := fmt.Sprintf("https://%s/%s/?format=json&values=false", discoveryService, d.Seed())
	klog.Infof("discovery: fetching peers from %s", url)
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, url, nil)
	if err != nil {
		d.cancel(fmt.Errorf("failed to create request: %w", err))
		return ""
	}
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := do(d.ctx, d.client(), req)
	if err != nil {
		d.cancel(fmt.Errorf("failed to do request: %w", err))
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		d.cancel(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
		return ""
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		d.cancel(fmt.Errorf("failed to read response body: %w", err))
		return ""
	}
	klog.Infof("discovery: peers raw body %s", body)
	var hostnames []string
	if err := json.Unmarshal(body, &hostnames); err != nil {
		d.cancel(fmt.Errorf("failed to decode response body: %w", err))
		return ""
	}
	if len(hostnames) == 0 {
		d.cancel(fmt.Errorf("no peers found"))
		return ""
	}
	peers := strings.Join(hostnames, ",")
	klog.Infof("discovery: peers %s", peers)
	return peers
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

// do sends req with an honest User-Agent and retries with linear backoff on
// rate-limit (429) and network errors, honoring any Retry-After header. Any
// other response (including 5xx) is returned so the caller can act on it. The
// request body is rewound between attempts via req.GetBody (set automatically
// by http.NewRequestWithContext for *strings.Reader bodies). On ctx
// cancellation it stops and returns the context's cancel cause.
func do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", userAgent)

	sleep := 0 * time.Second
	for {
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("rewind body: %w", err)
			}
			req.Body = body
		}

		resp, err := client.Do(req)
		// Only rate-limit (429) is retried server-side; any other response
		// (including 5xx) is returned so the caller can cancel on it.
		if err == nil && resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}

		// Stop retrying once the context is done. client.Do wraps cancellation
		// as a generic "context canceled"; surface the real cancel cause instead.
		if ctx.Err() != nil {
			if resp != nil {
				resp.Body.Close()
			}
			return nil, context.Cause(ctx)
		}

		if err == nil {
			if ra := retryAfter(resp.Header.Get("Retry-After")); ra > sleep {
				sleep = ra
			}
			klog.Warningf("discovery: %s returned %d, retrying...", discoveryService, resp.StatusCode)
			resp.Body.Close()
		} else {
			klog.Warningf("discovery: %s request failed: %v, retrying...", discoveryService, err)
		}

		sleep += 1 * time.Second
		<-await(ctx, time.After(sleep))
	}
}

// retryAfter parses a Retry-After header value, either delay-seconds or an
// HTTP-date. Returns 0 if absent or unparseable.
func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
