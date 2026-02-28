package docker

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/utils/exec"
)

// StreamingRuntime implements streaming.Runtime for exec/attach/portforward
// by delegating to the Docker Engine API.
type StreamingRuntime struct {
	backend *Backend
}

// NewStreamingRuntime creates a StreamingRuntime from a Backend.
// The backend's client is accessed lazily, so this can be called before Init().
func NewStreamingRuntime(b *Backend) *StreamingRuntime {
	return &StreamingRuntime{backend: b}
}

func (s *StreamingRuntime) Exec(ctx context.Context, containerID string, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdin:  stdin != nil,
		AttachStdout: stdout != nil,
		AttachStderr: stderr != nil,
		Tty:          tty,
	}

	logger.Info().Str("container", containerID[:12]).Strs("cmd", cmd).Bool("tty", tty).Msg("exec")

	createResp, err := s.backend.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return err
	}

	attachResp, err := s.backend.client.ContainerExecAttach(ctx, createResp.ID, container.ExecAttachOptions{Tty: tty})
	if err != nil {
		return err
	}
	defer attachResp.Close()

	// Handle terminal resize
	if tty && resize != nil {
		go func() {
			for size := range resize {
				s.backend.client.ContainerExecResize(ctx, createResp.ID, container.ResizeOptions{
					Height: uint(size.Height),
					Width:  uint(size.Width),
				})
			}
		}()
	}

	proxyStreams(tty, stdin, stdout, stderr, attachResp)

	// Wait for Docker to acknowledge exec completion and return the exit code.
	inspect, err := s.backend.client.ContainerExecInspect(ctx, createResp.ID)
	if err != nil {
		return nil
	}
	if inspect.ExitCode != 0 {
		logger.Info().Str("container", containerID[:12]).Int("code", inspect.ExitCode).Msg("exec exited")
		return utilexec.CodeExitError{
			Err:  fmt.Errorf("command terminated with exit code %d", inspect.ExitCode),
			Code: inspect.ExitCode,
		}
	}
	return nil
}

func (s *StreamingRuntime) Attach(ctx context.Context, containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	opts := container.AttachOptions{
		Stream: true,
		Stdin:  stdin != nil,
		Stdout: stdout != nil,
		Stderr: stderr != nil,
	}

	logger.Info().Str("container", containerID[:12]).Msg("attach")

	attachResp, err := s.backend.client.ContainerAttach(ctx, containerID, opts)
	if err != nil {
		return err
	}
	defer attachResp.Close()

	// Handle terminal resize
	if tty && resize != nil {
		go func() {
			for size := range resize {
				s.backend.client.ContainerResize(ctx, containerID, container.ResizeOptions{
					Height: uint(size.Height),
					Width:  uint(size.Width),
				})
			}
		}()
	}

	return proxyStreams(tty, stdin, stdout, stderr, attachResp)
}

func (s *StreamingRuntime) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	inspect, err := s.backend.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return err
	}

	ip := getIPFromInspect(inspect)
	if ip == "" {
		return fmt.Errorf("no IP address for sandbox %s", podSandboxID)
	}

	logger.Info().Str("sandbox", podSandboxID[:12]).Int32("port", port).Str("ip", ip).Msg("port-forward")

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Bidirectional proxy
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(conn, stream)
	}()
	go func() {
		defer wg.Done()
		io.Copy(stream, conn)
		stream.Close()
	}()
	wg.Wait()
	return nil
}

// proxyStreams proxies stdin/stdout/stderr to/from a hijacked Docker connection.
// HijackedResponse has Conn (net.Conn) for writing and Reader (*bufio.Reader) for reading.
func proxyStreams(tty bool, stdin io.Reader, stdout, stderr io.WriteCloser, resp dockertypes.HijackedResponse) error {
	var wg sync.WaitGroup

	if stdin != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			io.Copy(resp.Conn, stdin)
			resp.CloseWrite()
		}()
	}

	if tty {
		// TTY mode: single stream, copy to stdout
		if stdout != nil {
			io.Copy(stdout, resp.Reader)
			stdout.Close()
		}
	} else {
		// Non-TTY: Docker multiplexes stdout/stderr with 8-byte header
		stdcopy.StdCopy(stdout, stderr, resp.Reader)
		if stdout != nil {
			stdout.Close()
		}
		if stderr != nil {
			stderr.Close()
		}
	}

	// Output is done (process exited). Close stdin and the Docker
	// connection to unblock the stdin goroutine which may be blocked
	// reading from the upstream stream or writing to Docker.
	if closer, ok := stdin.(io.Closer); ok {
		closer.Close()
	}
	resp.Conn.Close()

	wg.Wait()
	return nil
}
