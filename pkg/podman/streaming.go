package podman

import (
	"context"
	"io"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubelet/pkg/cri/streaming"
)

type streamingRuntimeImpl struct {
	driver v1.Driver
}

var _ streaming.Runtime = &streamingRuntimeImpl{}

func NewStreaming(driver v1.Driver) streaming.Runtime {
	return &streamingRuntimeImpl{
		driver: driver,
	}
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
