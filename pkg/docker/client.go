package docker

import (
	"github.com/cnuss/nanokube/pkg"
	dockerclient "github.com/docker/docker/client"
)

func newClient(_ pkg.Config, socket string) (*dockerclient.Client, error) {
	// httpClient := &http.Client{
	// 	Transport: newTransport(config, socket),
	// }
	return dockerclient.NewClientWithOpts(dockerclient.WithHost("unix://"+socket), dockerclient.WithAPIVersionNegotiation() /*, dockerclient.WithHTTPClient(httpClient)*/)
}

// type roundTripFunc func(*http.Request) (*http.Response, error)

// func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// type readCloserFunc struct {
// 	io.Reader
// 	close func() error
// }

// func (r readCloserFunc) Close() error { return r.close() }

// func newTransport(config pkg.Config, socket string) http.RoundTripper {
// 	inner := &http.Transport{
// 		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
// 			return net.Dial("unix", socket)
// 		},
// 	}
// 	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
// 		// Tee request body at debug level
// 		var reqBuf bytes.Buffer
// 		if req.Body != nil && log.V(2).Enabled() {
// 			req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
// 		}

// 		resp, err := inner.RoundTrip(req)
// 		if err != nil {
// 			log.V(3).Info("dockerapi", "method", req.Method, "url", req.URL.String(), "error", err)
// 			return resp, err
// 		}

// 		// Wiretap response body at debug level, log on close
// 		if resp.Body != nil && log.V(2).Enabled() {
// 			var respBuf bytes.Buffer
// 			body := resp.Body
// 			resp.Body = readCloserFunc{
// 				Reader: io.TeeReader(body, &respBuf),
// 				close: func() error {
// 					err := body.Close()
// 					kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode, "respBody", respBuf.String()}
// 					if reqBuf.Len() > 0 {
// 						kv = append(kv, "reqBody", reqBuf.String())
// 					}
// 					log.V(2).Info("dockerapi", kv...)
// 					return err
// 				},
// 			}
// 		} else {
// 			kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode}
// 			if reqBuf.Len() > 0 {
// 				kv = append(kv, "reqBody", reqBuf.String())
// 			}
// 			log.V(3).Info("dockerapi", kv...)
// 		}

// 		return resp, nil
// 	})
// }
