package docker

import (
	"context"
	"net"
	"net/http"

	"github.com/cnuss/nanokube/pkg/nanokube"
	dockerclient "github.com/docker/docker/client"
	"k8s.io/klog/v2"
)

func newClient(ctx context.Context, socket string) (*dockerclient.Client, error) {
	httpClient := &http.Client{
		Transport: newTransport(ctx, socket),
	}
	return dockerclient.NewClientWithOpts(
		dockerclient.WithHost("unix://"+socket),
		dockerclient.WithAPIVersionNegotiation(),
		dockerclient.WithHTTPClient(httpClient),
	)
}

type ctxOverrideRoundTripper struct {
	ctx   context.Context
	inner http.RoundTripper
}

// RoundTrip swaps the caller's ctx with the backend's super ctx so docker
// requests outlive caller-side cancellation (e.g. kubelet housekeeping ctx
// being canceled during shutdown).
func (r ctxOverrideRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.inner.RoundTrip(req.WithContext(r.ctx))
}

func newTransport(ctx context.Context, socket string) http.RoundTripper {
	inner := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socket)
		},
	}
	logged := nanokube.HTTPLog{
		Name:    "dockerapi",
		Log:     klog.V(7).Enabled(),
		Verbose: klog.V(8).Enabled(),
	}.RoundTripper(inner)
	return ctxOverrideRoundTripper{ctx: ctx, inner: logged}
}
