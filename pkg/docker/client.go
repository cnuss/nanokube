package docker

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	dockerclient "github.com/docker/docker/client"
)

func newClient(config v1.Config, socket string) (*dockerclient.Client, error) {
	opts := []dockerclient.Opt{
		dockerclient.WithHost("unix://" + socket),
		dockerclient.WithAPIVersionNegotiation(),
	}
	if config.Options().Verbosity() >= 3 {
		httpClient := &http.Client{
			Transport: newTransport(config, socket),
		}
		opts = append(opts, dockerclient.WithHTTPClient(httpClient))
	}
	return dockerclient.NewClientWithOpts(opts...)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type readCloserFunc struct {
	io.Reader
	close func() error
}

func (r readCloserFunc) Close() error { return r.close() }

func newTransport(config v1.Config, socket string) http.RoundTripper {
	inner := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socket)
		},
	}
	verbose := config.Options().Verbosity() >= 4
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var reqBuf bytes.Buffer
		if req.Body != nil && verbose {
			req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
		}

		resp, err := inner.RoundTrip(req)
		if err != nil {
			nanokube.Log.Info("dockerapi", "method", req.Method, "url", req.URL.String(), "error", err)
			return resp, err
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
					nanokube.Log.Info("dockerapi", kv...)
					return err
				},
			}
		} else {
			kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode}
			if reqBuf.Len() > 0 {
				kv = append(kv, "reqBody", reqBuf.String())
			}
			nanokube.Log.Info("dockerapi", kv...)
		}

		return resp, nil
	})
}
