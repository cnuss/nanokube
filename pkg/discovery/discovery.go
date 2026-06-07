package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/tunnel"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"k8s.io/klog/v2"
)

// userAgent honestly identifies nanokube to kvdb.io.
const (
	userAgent        = "nanokube (+https://github.com/cnuss/nanokube)"
	discoveryService = "kvdb.io"
)

type Discovery interface {
	Seed() string
	Peers() types.URLsMap

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
				TLSHandshakeTimeout:   15 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
			},
			Timeout: 15 * time.Second,
		}
	})
	return d.http
}

func (d *discoveryImpl) WithTunnel(tunnel tunnel.Tunnel) Discovery {
	d.tunnelOnce.Do(func() {
		klog.Infof("discovery: registering tunnel %s with discovery service", tunnel.Hostname())
		payload, err := json.Marshal(tunnel.Spec())
		if err != nil {
			d.cancel(fmt.Errorf("failed to marshal tunnel spec: %w", err))
			return
		}

		url := fmt.Sprintf("https://%s/%s/peer:%s", discoveryService, d.Seed(), tunnel.Hostname())

		req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, url, bytes.NewReader(payload))
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
		create := func() (*string, error) {
			klog.Infof("discovery: registering with discovery service %s", discoveryService)

			os.Setenv("NANOKUBE_SEED", "")
			err := os.Remove(d.seedFile)
			if err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to remove seed file: %w", err)
			}

			url := fmt.Sprintf("https://%s", discoveryService)
			payload := strings.NewReader(fmt.Sprintf("email=%s@nanokube.xyz", discoveryService))

			req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, url, payload)
			if err != nil {
				return nil, fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			resp, err := do(d.ctx, d.client(), req)
			if err != nil {
				return nil, fmt.Errorf("failed to do request: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			location := resp.Header.Get("Location")
			if location == "" {
				return nil, fmt.Errorf("missing Location header")
			}
			seed := strings.Trim(path.Base(location), "/")
			return &seed, nil
		}

		getOrCreate := func() (*string, error) {
			var seed string
			if val := os.Getenv("NANOKUBE_SEED"); val != "" {
				seed = val
			}
			if val, err := os.ReadFile(d.seedFile); err == nil {
				seed = string(val)
			}
			if seed == "" {
				return create()
			}

			url := fmt.Sprintf("https://%s/%s/", discoveryService, seed)
			req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, url, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Cache-Control", "no-cache")
			resp, err := do(d.ctx, d.client(), req)
			if err != nil {
				return nil, fmt.Errorf("failed to do request: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return &seed, nil
			}
			if resp.StatusCode != http.StatusNotFound {
				return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}

			klog.Warningf("discovery: existing seed %s not found, creating new one", seed)
			return create()
		}

		seed, err := getOrCreate()
		if err != nil {
			d.cancel(err)
			return
		}
		err = os.WriteFile(d.seedFile, []byte(*seed), 0o600)
		if err != nil {
			klog.Errorf("discovery: failed to write seed file: %v", err)
			d.cancel(fmt.Errorf("failed to write seed file: %w", err))
			return
		}
		klog.Infof("discovery: obtained seed %s", *seed)
		os.Setenv("NANOKUBE_SEED", *seed)
		d.seed = *seed
	})
	return d.seed
}

func (d *discoveryImpl) Peers() types.URLsMap {
	peers := types.URLsMap{
		d.Tunnel().Hostname(): []url.URL{*d.Tunnel().URL()},
	}
	payload := url.Values{}
	payload.Set("prefix", "peer:")
	payload.Set("format", "json")
	payload.Set("values", "false")
	endpoint := fmt.Sprintf("https://%s/%s/?%s", discoveryService, d.Seed(), payload.Encode())

	klog.Infof("discovery: fetching peers for seed %s", d.Seed())
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return peers
	}
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := do(d.ctx, d.client(), req)
	if err != nil {
		return peers
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return peers
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return peers
	}
	var values []string
	if err := json.Unmarshal(body, &values); err != nil {
		return peers
	}
	if len(values) == 0 {
		return peers
	}
	for _, value := range values {
		peer := strings.TrimPrefix(value, "peer:")
		u, err := url.Parse(fmt.Sprintf("https://%s:443", peer))
		if err != nil {
			continue
		}
		peers[peer] = []url.URL{*u}
	}
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
