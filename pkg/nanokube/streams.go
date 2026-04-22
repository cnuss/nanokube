package nanokube

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/emicklei/go-restful/v3"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	remotecommandconsts "k8s.io/apimachinery/pkg/util/remotecommand"
	tools "k8s.io/client-go/tools/remotecommand"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming/portforward"
	"k8s.io/kubelet/pkg/cri/streaming/remotecommand"
	remotecommandserver "k8s.io/kubelet/pkg/cri/streaming/remotecommand"
)

type Streams interface {
	New() Stream
	Service() *restful.WebService
}

type StreamsImpl struct {
	driver  v1.Driver
	streams sync.Map // streamID -> *StreamImpl
}

var _ Streams = &StreamsImpl{}

func NewStreams(driver v1.Driver) Streams {
	return &StreamsImpl{
		driver: driver,
	}
}

func (s *StreamsImpl) Service() *restful.WebService {
	ws := new(restful.WebService)

	handler := func(req *restful.Request, resp *restful.Response) {
		streamID := req.PathParameter("streamID")
		value, ok := s.streams.Load(streamID)
		if !ok {
			resp.WriteError(http.StatusNotFound, fmt.Errorf("stream %s not found", streamID))
			return
		}
		stream, ok := value.(*StreamImpl)
		if !ok {
			resp.WriteError(http.StatusInternalServerError, fmt.Errorf("stream %s has invalid type", streamID))
			return
		}

		go func() {
			done := <-stream.Done()
			defer done.Cancel() // ensure any resources associated with the stream are cleaned up
			Log.Info("stream done", "streamID", streamID, "code", done.Code, "error", done.Err)
			s.streams.Delete(streamID)
		}()
		Log.Info("handling stream", "streamID", streamID)
		stream.WithTimeout(4*time.Hour).Handle(req, resp)
	}

	ws.Path(s.driver.BaseURL().JoinPath("streams").Path).
		Route(ws.POST("/{streamID}").To(handler)).
		Route(ws.GET("/{streamID}").To(handler))

	return ws
}

func (s *StreamsImpl) New() Stream {
	stream := &StreamImpl{
		id:           uuid.NewString(),
		driver:       s.driver,
		doneProvided: make(chan struct{}),
		doneReady:    make(chan struct{}),
	}
	s.streams.Store(stream.id, stream)
	return stream
}

type (
	ExecHandler    func(ctx context.Context, stream Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan tools.TerminalSize, timeout time.Duration) <-chan Done
	AttachHandler  func(ctx context.Context, stream Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan tools.TerminalSize) error
	ForwardHandler func(ctx context.Context, stream Stream, port int32, closer io.ReadWriteCloser) error
)

type Proxy struct {
	CloseWrite func() error
	Reader     *bufio.Reader
	Conn       net.Conn
}

type Stream interface {
	remotecommand.Executor
	remotecommand.Attacher
	portforward.PortForwarder

	ID() string
	URL() string
	Done() <-chan Done

	WithExec(request *criv1.ExecRequest, handler ExecHandler) Stream
	WithAttach(request *criv1.AttachRequest, handler AttachHandler) Stream
	WithForward(request *criv1.PortForwardRequest, handler ForwardHandler) Stream
	WithTimeout(timeout time.Duration) Stream
	WithDone(done <-chan Done) Stream

	Handle(req *restful.Request, resp *restful.Response)
	ProxyStream(ctx context.Context, tty bool, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, res *Proxy) (context.CancelFunc, error)
}

type Done struct {
	Code   int
	Err    error
	Cancel context.CancelFunc
}

type StreamImpl struct {
	id      string
	driver  v1.Driver
	handler http.Handler
	timeout time.Duration

	done         <-chan Done
	doneProvided chan struct{}
	doneReady    chan struct{}
	doneValue    Done

	exec        *criv1.ExecRequest
	execHandler ExecHandler

	attach        *criv1.AttachRequest
	attachHandler AttachHandler

	forward        *criv1.PortForwardRequest
	forwardHandler ForwardHandler
}

var _ Stream = &StreamImpl{}

func (s *StreamImpl) ID() string {
	return s.id
}

func (s *StreamImpl) Done() <-chan Done {
	ch := make(chan Done, 1)
	go func() {
		<-s.doneReady
		ch <- s.doneValue
		close(ch)
	}()
	return ch
}

func (s *StreamImpl) WithExec(request *criv1.ExecRequest, handler ExecHandler) Stream {
	s.exec = request
	s.execHandler = handler
	return s
}

func (s *StreamImpl) WithAttach(request *criv1.AttachRequest, handler AttachHandler) Stream {
	s.attach = request
	s.attachHandler = handler
	return s
}

func (s *StreamImpl) WithForward(request *criv1.PortForwardRequest, handler ForwardHandler) Stream {
	s.forward = request
	s.forwardHandler = handler
	return s
}

func (s *StreamImpl) WithTimeout(timeout time.Duration) Stream {
	s.timeout = timeout
	return s
}

func (s *StreamImpl) WithDone(done <-chan Done) Stream {
	go func() {
		s.doneValue = <-done // zero-value if sender closes without sending, fine
		close(s.doneReady)
	}()
	return s
}

func (s *StreamImpl) Handle(req *restful.Request, resp *restful.Response) {
	if s.exec != nil {
		remotecommandserver.ServeExec(
			resp.ResponseWriter, req.Request, s,
			"", // unused pod name
			"", // unused uid
			"", // unused container name
			s.exec.GetCmd(),
			&remotecommandserver.Options{
				Stdin:  true,
				Stdout: true,
				Stderr: true,
				TTY:    s.exec.GetTty(),
			},
			s.timeout,
			remotecommandconsts.DefaultStreamCreationTimeout,
			remotecommandconsts.SupportedStreamingProtocols,
		)
		return
	}

	if s.attach != nil {
		remotecommandserver.ServeAttach(
			resp.ResponseWriter, req.Request, s,
			"", // unused pod name
			"", // unused uid
			"", // unused container name
			&remotecommandserver.Options{
				Stdin:  true,
				Stdout: true,
				Stderr: true,
				TTY:    s.attach.GetTty(),
			},
			s.timeout,
			remotecommandconsts.DefaultStreamCreationTimeout,
			remotecommandconsts.SupportedStreamingProtocols,
		)
		return
	}

	if s.forward != nil {
		portforward.ServePortForward(
			resp.ResponseWriter, req.Request, s,
			"", // unused pod name
			"", // unused uid
			&portforward.V4Options{
				Ports: s.forward.GetPort(),
			},
			s.timeout,
			remotecommandconsts.DefaultStreamCreationTimeout,
			remotecommandconsts.SupportedStreamingProtocols,
		)
		return
	}

	http.Error(resp.ResponseWriter, fmt.Sprintf("stream %s: no exec/attach/forward handler configured", s.id), http.StatusInternalServerError)
}

func (s *StreamImpl) URL() string {
	return fmt.Sprintf("%s/streams/%s", s.driver.BaseURL().String(), s.id)
}

func (s *StreamImpl) AttachContainer(ctx context.Context, _ string, _ types.UID, _ string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan tools.TerminalSize) error {
	if s.attach == nil || s.attachHandler == nil {
		return fmt.Errorf("stream %s: attach handler not configured", s.id)
	}

	return s.attachHandler(ctx, s, s.attach.Stdin, in, s.attach.Stdout, out, s.attach.Stderr, err, resize)
}

func (s *StreamImpl) ExecInContainer(ctx context.Context, _ string, _ types.UID, _ string, cmd []string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan tools.TerminalSize, timeout time.Duration) error {
	if s.exec == nil || s.execHandler == nil {
		return fmt.Errorf("stream %s: exec handler not configured", s.id)
	}

	if timeout <= 0 {
		timeout = s.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := <-s.WithDone(s.execHandler(ctx, s, s.exec.Stdin, in, s.exec.Stdout, out, s.exec.Stderr, err, resize, timeout)).Done()

	if done.Err != nil {
		return NewError(done.Err).WithCode(done.Code)
	}

	return nil
}

func (s *StreamImpl) PortForward(ctx context.Context, _ string, _ types.UID, port int32, stream io.ReadWriteCloser) error {
	if s.forward == nil || s.forwardHandler == nil {
		return fmt.Errorf("stream %s: forward handler not configured", s.id)
	}

	return s.forwardHandler(ctx, s, port, stream)
}

func (s *StreamImpl) ProxyStream(ctx context.Context, tty bool, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, res *Proxy) (context.CancelFunc, error) {
	defer res.CloseWrite()
	defer res.Conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	stdoutDone := make(chan struct{})
	var outErr, errErr error

	// Non-tty: stdcopy (in the stdout goroutine) demuxes stderr frames into
	// errPipeW; the stderr goroutine drains errPipeR to the client's err.
	// Tty: no stderr on the wire; the pipe stays unused.
	var errPipeR *io.PipeReader
	var errPipeW *io.PipeWriter
	if !tty {
		errPipeR, errPipeW = io.Pipe()
	}

	// stdin: copy to conn if the client asked for it, else drain to /dev/null
	// so the client side doesn't stall on a full buffer. AfterFunc closes the
	// source on ctx cancel to unblock the parked Read.
	go func() {
		if closer, ok := in.(io.Closer); ok {
			stop := context.AfterFunc(ctx, func() {
				closer.Close()
			})
			defer stop()
		}
		dst := io.Writer(io.Discard)
		if stdin {
			dst = res.Conn
		}
		io.Copy(dst, in)
	}()

	// stderr: drains the demux pipe in non-tty mode; in tty mode there's
	// nothing on the wire so the goroutine just holds the pessimistic close
	// hook until ctx cancel.
	go func() {
		stop := context.AfterFunc(ctx, func() {
			err.Close()
		})
		defer stop()
		if tty {
			<-ctx.Done()
			return
		}
		dst := io.Writer(io.Discard)
		if stderr {
			dst = err
		}
		_, errErr = io.Copy(dst, errPipeR)
	}()

	// stdout: sole reader of res.Reader. tty => single stream. non-tty =>
	// stdcopy demuxes stdout→out and stderr→errPipeW (drained by the stderr
	// goroutine). Closing errPipeW on the way out lets that goroutine finish.
	go func() {
		defer close(stdoutDone)
		if tty {
			dst := io.Writer(io.Discard)
			if stdout {
				dst = out
			}
			_, outErr = io.Copy(dst, res.Reader)
			return
		}
		outDst := io.Writer(io.Discard)
		if stdout {
			outDst = out
		}
		_, outErr = stdcopy.StdCopy(outDst, errPipeW, res.Reader)
		errPipeW.Close()
	}()

	<-stdoutDone
	// TODO(research): some race condition between stdout closing and the response finishing
	// TODO(research): extra newline on -it + sh
	time.Sleep(1 * time.Second)

	errs := []error{}
	if outErr != nil {
		errs = append(errs, fmt.Errorf("output: %w", outErr))
	}
	if errErr != nil {
		errs = append(errs, fmt.Errorf("stderr: %w", errErr))
	}

	return cancel, errors.Join(errs...)
}
