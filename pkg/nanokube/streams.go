package nanokube

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/google/uuid"
	remotecommandconsts "k8s.io/apimachinery/pkg/util/remotecommand"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	"k8s.io/cri-streaming/pkg/streaming/remotecommand"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
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
			// wait for cleanup and delete the stream reference
			done := <-stream.Done()
			defer done.Cancel()
			s.streams.Delete(streamID)
		}()
		stream.WithTimeout(4*time.Hour).Handle(req, resp)
	}

	ws.Path(s.driver.BaseURL().JoinPath("streams").Path).
		Route(ws.POST("/{streamID}").To(handler).Operation("connectPostStream")).
		Route(ws.GET("/{streamID}").To(handler).Operation("connectGetStream"))

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
	ExecHandler    func(ctx context.Context, stream Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan remotecommand.TerminalSize, timeout time.Duration) <-chan Done
	AttachHandler  func(ctx context.Context, stream Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan remotecommand.TerminalSize) <-chan Done
	ForwardHandler func(ctx context.Context, stream Stream, port int32, closer io.ReadWriteCloser) <-chan Done
)

type Proxy struct {
	CloseWrite func() error
	Reader     io.Reader
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
	Resizer(ctx context.Context, resize <-chan remotecommand.TerminalSize) <-chan Resizer
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

func (s *StreamImpl) AttachContainer(ctx context.Context, _ string, _ string, _ string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	if s.attach == nil || s.attachHandler == nil {
		return fmt.Errorf("stream %s: attach handler not configured", s.id)
	}

	done := <-s.WithDone(s.attachHandler(ctx, s, s.attach.Stdin, in, s.attach.Stdout, out, s.attach.Stderr, err, resize)).Done()
	if done.Err != nil {
		return NewError(done.Err).WithCode(done.Code)
	}

	return nil
}

func (s *StreamImpl) ExecInContainer(ctx context.Context, _ string, _ string, _ string, cmd []string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize, timeout time.Duration) error {
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

func (s *StreamImpl) PortForward(ctx context.Context, _ string, _ string, port int32, stream io.ReadWriteCloser) error {
	if s.forward == nil || s.forwardHandler == nil {
		return fmt.Errorf("stream %s: forward handler not configured", s.id)
	}

	done := <-s.WithDone(s.forwardHandler(ctx, s, port, stream)).Done()

	if done.Err != nil {
		return NewError(done.Err).WithCode(done.Code)
	}

	return nil
}

func (s *StreamImpl) ProxyStream(ctx context.Context, tty bool, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, res *Proxy) (context.CancelFunc, error) {
	defer res.CloseWrite()
	defer res.Conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	outputDone := make(chan struct{})
	var outputErr error

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

	// output: sole reader of res.Reader. tty => raw single stream (no framing);
	// non-tty => Docker's stdcopy wire format: [type:1][zero:3][size:4BE][payload].
	// type 1 = stdout, 2 = stderr.
	go func() {
		defer close(outputDone)
		defer out.Close()
		defer err.Close()

		if tty {
			dst := io.Writer(io.Discard)
			if stdout {
				dst = out
			}
			_, outputErr = io.Copy(dst, res.Reader)
			return
		}

		header := make([]byte, 8)
		for {
			if _, rerr := io.ReadFull(res.Reader, header); rerr != nil {
				if !errors.Is(rerr, io.EOF) {
					outputErr = rerr
				}
				return
			}
			size := binary.BigEndian.Uint32(header[4:8])
			payload := make([]byte, size)
			if _, rerr := io.ReadFull(res.Reader, payload); rerr != nil {
				outputErr = rerr
				return
			}
			var dst io.Writer = io.Discard
			switch header[0] {
			case 1:
				if stdout {
					dst = out
				}
			case 2:
				if stderr {
					dst = err
				}
			}
			if _, werr := dst.Write(payload); werr != nil {
				outputErr = werr
				return
			}
		}
	}()

	<-outputDone
	// TODO(research): some race condition between stdout closing and the response finishing
	time.Sleep(1 * time.Second)

	if outputErr != nil {
		return cancel, fmt.Errorf("output: %w", outputErr)
	}
	return cancel, nil
}

// TODO(incomplete): move
type Resizer interface {
	TerminalSize() remotecommand.TerminalSize
	ConsoleSize() *[2]uint
	WithHandler(func(height, width uint)) Resizer
	Done() error
}

type resizerImpl struct {
	cancelFunc   context.CancelFunc
	mu           sync.Mutex
	terminalSize remotecommand.TerminalSize
	consoleSize  *[2]uint
	handler      func(height, width uint)
}

var _ Resizer = &resizerImpl{}

// initialResizeTimeout bounds the wait for the client's first TerminalSize
// event. Conformant clients (e.g. kubectl) prime their size queue with the
// current terminal size, so in practice the first event arrives well under
// this budget. Sparse clients fall through and attach proceeds unsized.
const initialResizeTimeout = 250 * time.Millisecond

func (r *resizerImpl) update(s remotecommand.TerminalSize) {
	r.mu.Lock()
	r.terminalSize = s
	r.consoleSize = &[2]uint{uint(s.Height), uint(s.Width)}
	h := r.handler
	r.mu.Unlock()
	if h != nil {
		h(uint(s.Height), uint(s.Width))
	}
}

func (r *resizerImpl) TerminalSize() remotecommand.TerminalSize {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminalSize
}

func (r *resizerImpl) ConsoleSize() *[2]uint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.consoleSize
}

func (r *resizerImpl) WithHandler(handler func(height, width uint)) Resizer {
	r.mu.Lock()
	r.handler = handler
	last := r.terminalSize
	replay := handler != nil && r.consoleSize != nil
	r.mu.Unlock()
	if replay {
		handler(uint(last.Height), uint(last.Width))
	}
	return r
}

func (r *resizerImpl) Done() error {
	r.cancelFunc()
	return nil
}

func (s *StreamImpl) Resizer(ctx context.Context, resize <-chan remotecommand.TerminalSize) <-chan Resizer {
	ctx, cancel := context.WithCancel(ctx)
	r := &resizerImpl{cancelFunc: cancel}
	ready := make(chan Resizer, 1)

	go func() {
		select {
		case s, ok := <-resize:
			if ok {
				r.update(s)
			}
		case <-time.After(initialResizeTimeout):
		case <-ctx.Done():
		}
		ready <- r

		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-resize:
				if !ok {
					return
				}
				r.update(s)
			}
		}
	}()
	return ready
}
