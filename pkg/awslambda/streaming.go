package awslambda

import (
	"context"
	"io"
	"net/url"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/client-go/tools/remotecommand"
)

type streamingRuntimeImpl struct {
	driver  v1.Driver
	baseURL *url.URL
}

var _ v1.StreamRuntime = &streamingRuntimeImpl{}

func NewStreamRuntime(driver v1.Driver, baseURL *url.URL) v1.StreamRuntime {
	return &streamingRuntimeImpl{
		driver:  driver,
		baseURL: baseURL,
	}
}

func (s *streamingRuntimeImpl) URL() *url.URL {
	return s.baseURL
}

func (s *streamingRuntimeImpl) Attach(ctx context.Context, containerID string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	panic("unimplemented")
}

func (s *streamingRuntimeImpl) Exec(ctx context.Context, containerID string, cmd []string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	panic("unimplemented")
}

func (s *streamingRuntimeImpl) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	panic("unimplemented")
}
