package docker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/remotecommand"
)

// execExitError satisfies k8s.io/utils/exec.ExitError so that non-zero exit
// codes flow through remotecommandserver.ServeExec as a NonZeroExitCode status.
type execExitError struct {
	code int
}

func (e *execExitError) Error() string   { return fmt.Sprintf("exit code %d", e.code) }
func (e *execExitError) Exited() bool    { return true }
func (e *execExitError) ExitStatus() int { return e.code }
func (e *execExitError) String() string  { return e.Error() }

// dockerExecutor implements k8s.io/kubelet/pkg/cri/streaming/remotecommand.Executor
// against a Docker container. The container id + cmd + stream flags are bound at
// construction time from the CRI ExecRequest; the pod/uid/container/cmd passed
// by ServeExec are ignored because they originate from the REST path, not the
// CRI ExecRequest.
type dockerExecutor struct {
	client      *client.Client
	containerID string
	cmd         []string
	stdin       bool
	stdout      bool
	stderr      bool
	tty         bool
}

func (e *dockerExecutor) ExecInContainer(ctx context.Context, _ string, _ types.UID, _ string, _ []string, in io.Reader, out, errw io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize, _ time.Duration) error {
	createResp, err := e.client.ContainerExecCreate(ctx, e.containerID, container.ExecOptions{
		Tty:          e.tty,
		AttachStdin:  e.stdin,
		AttachStdout: e.stdout,
		AttachStderr: e.stderr,
		Cmd:          e.cmd,
	})
	if err != nil {
		return fmt.Errorf("exec create: %w", err)
	}

	attachResp, err := e.client.ContainerExecAttach(ctx, createResp.ID, container.ExecAttachOptions{Tty: e.tty})
	if err != nil {
		return fmt.Errorf("exec attach: %w", err)
	}
	defer attachResp.Close()

	if resize != nil {
		go func() {
			for size := range resize {
				if err := e.client.ContainerExecResize(ctx, createResp.ID, container.ResizeOptions{Height: uint(size.Height), Width: uint(size.Width)}); err != nil {
					nanokube.Log.Warn("exec resize failed", "execID", createResp.ID, "err", err)
				}
			}
		}()
	}

	// Only wire up stdin and invoke CloseWrite if the client actually requested it.
	// Calling CloseWrite on an attach connection that never had stdin attached is
	// interpreted by some docker backends as "detach" and tears the exec down before
	// output is produced.
	if e.stdin && in != nil {
		go func() {
			_, _ = io.Copy(attachResp.Conn, in)
			_ = attachResp.CloseWrite()
		}()
	}

	if e.tty {
		// TTY multiplexes stdout/stderr onto a single PTY; write everything to out.
		if out != nil {
			_, _ = io.Copy(out, attachResp.Reader)
		}
	} else {
		// Docker frames stdout/stderr; demux with stdcopy.
		_, _ = stdcopy.StdCopy(discardIfNil(out), discardIfNil(errw), attachResp.Reader)
	}

	inspect, err := e.client.ContainerExecInspect(ctx, createResp.ID)
	if err != nil {
		return fmt.Errorf("exec inspect: %w", err)
	}

	// Cloudflare's quick-tunnel QUIC origin appears to buffer small WebSocket
	// frames and drop in-flight data when the TCP origin conn closes. Pause
	// briefly after the last write so the trailing status frame + our data
	// actually reach the client before ServeExec's defer ctx.conn.Close() tears
	// the TCP conn down. Without this, kubectl observes "1006 abnormal closure".
	time.Sleep(execDrainDelay)

	if inspect.ExitCode != 0 {
		return &execExitError{code: inspect.ExitCode}
	}
	return nil
}

// execDrainDelay is how long ExecInContainer waits after the last stream write
// before returning. See comment in ExecInContainer for rationale.
const execDrainDelay = 2 * time.Second

func discardIfNil(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
}
