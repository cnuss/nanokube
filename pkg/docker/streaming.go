package docker

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"k8s.io/client-go/tools/remotecommand"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	utilexec "k8s.io/utils/exec"
)

type streamingRuntimeImpl struct {
	client  *client.Client
	driver  v1.Driver
	baseURL *url.URL
}

var _ v1.StreamRuntime = &streamingRuntimeImpl{}

func NewStreamRuntime(client *client.Client, driver v1.Driver, baseURL *url.URL) v1.StreamRuntime {
	return &streamingRuntimeImpl{
		client:  client,
		driver:  driver,
		baseURL: baseURL,
	}
}

func (s *streamingRuntimeImpl) URL() *url.URL {
	return s.baseURL
}

func (s *streamingRuntimeImpl) Attach(ctx context.Context, containerID string, stdin io.Reader, stdout io.WriteCloser, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	status, err := s.driver.ContainerStatus(ctx, containerID, true)
	if err != nil {
		return err
	}
	if status.GetStatus().GetState() != criv1.ContainerState_CONTAINER_RUNNING {
		return fmt.Errorf("container %s is not running", containerID)
	}

	attach, err := s.client.ContainerAttach(ctx, containerID, container.AttachOptions{
		Stream: true,
		Stdin:  stdin != nil,
		Stdout: stdout != nil,
		Stderr: stderr != nil,
	})
	if err != nil {
		return err
	}
	defer attach.Close()

	proxyStreams(tty, stdin, stdout, stderr, attach, resize, func(opts container.ResizeOptions) error {
		return s.client.ContainerResize(ctx, containerID, opts)
	})

	return nil
}

func (s *streamingRuntimeImpl) Exec(ctx context.Context, containerID string, cmd []string, stdin io.Reader, stdout io.WriteCloser, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	status, err := s.driver.ContainerStatus(ctx, containerID, true)
	if err != nil {
		return err
	}
	if status.GetStatus().GetState() != criv1.ContainerState_CONTAINER_RUNNING {
		return fmt.Errorf("container %s is not running", containerID)
	}

	created, err := s.client.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd:          cmd,
		AttachStdin:  stdin != nil,
		AttachStdout: stdout != nil,
		AttachStderr: stderr != nil,
		Tty:          tty,
	})
	if err != nil {
		return err
	}

	attach, err := s.client.ContainerExecAttach(ctx, created.ID, container.ExecAttachOptions{Tty: tty})
	if err != nil {
		return err
	}
	defer attach.Close()

	proxyStreams(tty, stdin, stdout, stderr, attach, resize, func(opts container.ResizeOptions) error {
		return s.client.ContainerExecResize(ctx, created.ID, opts)
	})

	inspect, err := s.client.ContainerExecInspect(ctx, created.ID)
	if err != nil {
		return nil
	}
	if inspect.ExitCode != 0 {
		return utilexec.CodeExitError{
			Err:  fmt.Errorf("command terminated with exit code %d", inspect.ExitCode),
			Code: inspect.ExitCode,
		}
	}
	return nil
}

func (s *streamingRuntimeImpl) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	_, err := s.driver.PodSandboxStatus(ctx, podSandboxID, true)
	if err != nil {
		return err
	}
	panic("unimplemented")
}

func proxyStreams(tty bool, stdin io.Reader, stdout, stderr io.WriteCloser, attach dockertypes.HijackedResponse, resize <-chan remotecommand.TerminalSize, resizeFn func(container.ResizeOptions) error) {
	if tty && resize != nil {
		go func() {
			for size := range resize {
				resizeFn(container.ResizeOptions{Height: uint(size.Height), Width: uint(size.Width)})
			}
		}()
	}

	var wg sync.WaitGroup
	if stdin != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			io.Copy(attach.Conn, stdin)
			attach.CloseWrite()
		}()
	}
	if tty {
		if stdout != nil {
			io.Copy(stdout, attach.Reader)
			stdout.Close()
		}
	} else {
		stdcopy.StdCopy(stdout, stderr, attach.Reader)
		if stdout != nil {
			stdout.Close()
		}
		if stderr != nil {
			stderr.Close()
		}
	}
	if closer, ok := stdin.(io.Closer); ok {
		closer.Close()
	}
	attach.Conn.Close()
	wg.Wait()
}
