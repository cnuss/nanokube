package docker

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	dockerclient "github.com/docker/docker/client"
	"k8s.io/klog/v2"
)

func newClient(ctx context.Context, nano v1.Nanokube, socket string) (*dockerclient.Client, error) {
	httpClient := &http.Client{
		Transport: newTransport(ctx, nano, socket),
	}
	return dockerclient.NewClientWithOpts(
		dockerclient.WithHost("unix://"+socket),
		dockerclient.WithAPIVersionNegotiation(),
		dockerclient.WithHTTPClient(httpClient),
	)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type readCloserFunc struct {
	io.Reader
	close func() error
}

func (r readCloserFunc) Close() error { return r.close() }

func newTransport(ctx context.Context, nano v1.Nanokube, socket string) http.RoundTripper {
	inner := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socket)
		},
	}
	logEnabled := nano.Options().Verbosity() >= 3
	verbose := nano.Options().Verbosity() >= 4
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		// Replace the caller's ctx with the backend's super ctx so the docker
		// connection's lifetime is independent of any caller-side shutdown
		// (e.g. kubelet's housekeeping ctx getting canceled during shutdown).
		req = req.WithContext(ctx)

		var reqBuf bytes.Buffer
		if req.Body != nil && verbose {
			req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
		}

		resp, err := inner.RoundTrip(req)
		if err != nil {
			if logEnabled {
				klog.ErrorS(err, "dockerapi", "method", req.Method, "url", req.URL.String())
			}
			return resp, err
		}

		if !logEnabled {
			return resp, nil
		}

		if resp.Body != nil && verbose {
			var respBuf bytes.Buffer
			body := resp.Body
			resp.Body = readCloserFunc{
				Reader: io.TeeReader(body, &respBuf),
				close: func() error {
					err := body.Close()
					kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode, "respBody", respBuf.String()}
					if reqBuf.Len() > 0 {
						kv = append(kv, "reqBody", reqBuf.String())
					}
					klog.InfoS("dockerapi", kv...)
					return err
				},
			}
		} else {
			kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode}
			if reqBuf.Len() > 0 {
				kv = append(kv, "reqBody", reqBuf.String())
			}
			klog.InfoS("dockerapi", kv...)
		}

		return resp, nil
	})
}
