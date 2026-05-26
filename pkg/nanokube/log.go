package nanokube

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/felixge/httpsnoop"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

// HTTPLog wraps a client RoundTripper or server Handler to klog request
// method/url/status (when Log) and request/response bodies (when Verbose).
// Body teeing buffers in memory and only emits on Close — avoid Verbose for
// long-lived streaming endpoints (exec, attach, watches).
type HTTPLog struct {
	Name    string
	Log     bool
	Verbose bool
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type readCloserFunc struct {
	io.Reader
	close func() error
}

func (r readCloserFunc) Close() error { return r.close() }

// RoundTripper returns inner wrapped with request/response logging.
func (h HTTPLog) RoundTripper(inner http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var reqBuf bytes.Buffer
		if req.Body != nil && h.Verbose {
			req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
		}
		resp, err := inner.RoundTrip(req)
		if err != nil {
			if h.Log {
				klog.ErrorS(err, h.Name, "method", req.Method, "url", req.URL.String())
			}
			return resp, err
		}
		if !h.Log {
			return resp, nil
		}
		h.logResponse(req, resp, &reqBuf)
		return resp, nil
	})
}

// Handler returns inner wrapped with request/response logging.
// Uses httpsnoop so the wrapped ResponseWriter exposes exactly the same set
// of optional interfaces (Flusher, Hijacker, CloseNotifier, etc.) as the
// caller's writer — otherwise deeper middleware that does
// `w.(http.Flusher)` checks would see false negatives for streaming
// endpoints (watch, exec) and refuse to serve.
func (h HTTPLog) Handler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var reqBuf bytes.Buffer
		if req.Body != nil && h.Verbose {
			req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
		}
		var (
			status  = http.StatusOK
			respBuf bytes.Buffer
		)
		snoopHooks := httpsnoop.Hooks{
			WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return func(code int) {
					status = code
					next(code)
				}
			},
		}
		if h.Verbose {
			snoopHooks.Write = func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return func(b []byte) (int, error) {
					respBuf.Write(b)
					return next(b)
				}
			}
		}
		wrapped := httpsnoop.Wrap(w, snoopHooks)
		inner.ServeHTTP(wrapped, req)
		if !h.Log {
			return
		}
		kv := []any{"method", req.Method, "url", req.URL.String(), "status", status}
		if reqBuf.Len() > 0 {
			kv = append(kv, "reqBody", reqBuf.String())
		}
		if respBuf.Len() > 0 {
			kv = append(kv, "respBody", respBuf.String())
		}
		klog.InfoS(h.Name, kv...)
	})
}

func (h HTTPLog) logResponse(req *http.Request, resp *http.Response, reqBuf *bytes.Buffer) {
	if resp.Body != nil && h.Verbose {
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
				klog.InfoS(h.Name, kv...)
				return err
			},
		}
		return
	}
	kv := []any{"method", req.Method, "url", req.URL.String(), "status", resp.StatusCode}
	if reqBuf.Len() > 0 {
		kv = append(kv, "reqBody", reqBuf.String())
	}
	klog.InfoS(h.Name, kv...)
}

// maxLineBytes caps the per-stream line buffer. If a container writes more
// than this without a newline, the buffered bytes are force-flushed as a line
// so the pump can never OOM the host on a misbehaving workload.
const maxLineBytes = 64 * 1024

// tap routes writes to io.Discard by default. On first Reader() call it
// lazily creates an os.Pipe and swaps the discard target for the pipe
// writer — bytes written before the first Reader() call are dropped;
// bytes after go into the kernel pipe buffer (non-blocking up to ~16KB).
type tap struct {
	w    atomic.Pointer[io.Writer]
	r    *os.File
	pipe *os.File
	once sync.Once
}

func newTap() *tap {
	t := &tap{}
	var discard io.Writer = io.Discard
	t.w.Store(&discard)
	return t
}

func (t *tap) Write(b []byte) (int, error) {
	return (*t.w.Load()).Write(b)
}

func (t *tap) Reader() io.ReadCloser {
	t.once.Do(func() {
		r, w, _ := os.Pipe()
		t.r = r
		t.pipe = w
		var writer io.Writer = w
		t.w.Store(&writer)
	})
	return t.r
}

func (t *tap) Close() {
	if t.pipe != nil {
		t.pipe.Close()
	}
	if t.r != nil {
		t.r.Close()
	}
}

// NewLogStream constructs a LogStream backed by the given source. The status
// provides LogPath for CRI log-file output; pass nil to skip file writing.
func NewLogStream(ctx context.Context, source LogSource, status *runtime.ContainerStatus) v1.LogStream {
	return &LogStreamImpl{
		ctx:    ctx,
		source: source,
		status: status,
		stdout: newTap(),
		stderr: newTap(),
	}
}

type LogStreamImpl struct {
	ctx    context.Context
	source LogSource
	status *runtime.ContainerStatus

	stdout *tap
	stderr *tap

	mu          sync.Mutex
	cancelFn    context.CancelFunc
	closeSource func()
	logFile     *os.File

	started, stopped, destroyed atomic.Bool
}

var _ v1.LogStream = &LogStreamImpl{}

func (s *LogStreamImpl) Stdout() io.ReadCloser { return s.stdout.Reader() }
func (s *LogStreamImpl) Stderr() io.ReadCloser { return s.stderr.Reader() }

func (s *LogStreamImpl) Start() {
	if s.destroyed.Load() {
		return
	}
	s.mu.Lock()
	s.started.Store(true)
	s.stopped.Store(false)

	if s.status != nil && s.status.LogPath != "" {
		if err := os.MkdirAll(filepath.Dir(s.status.LogPath), 0o755); err != nil {
			klog.ErrorS(err, "logstream: failed to create log dir", "path", s.status.LogPath)
			s.mu.Unlock()
			s.Stop()
			return
		}
		f, err := os.OpenFile(s.status.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			klog.ErrorS(err, "logstream: failed to open log file", "path", s.status.LogPath)
			s.mu.Unlock()
			s.Stop()
			return
		}
		s.logFile = f
	}

	ctx, cancel := context.WithCancel(s.ctx)
	s.cancelFn = cancel

	stdoutSrc, stderrSrc, closeSrc, err := s.source(ctx)
	if err != nil {
		klog.ErrorS(err, "logstream: source failed")
		s.mu.Unlock()
		s.Stop()
		return
	}
	s.closeSource = closeSrc
	s.mu.Unlock()

	go s.pump(stdoutSrc, "stdout", s.stdout)
	go s.pump(stderrSrc, "stderr", s.stderr)
}

// pump reads CRI-formatted bytes from src (already shaped by the runtime's
// LogSource as "<ts> <stream> F <msg>\n"), splits on newlines, and writes each
// line to the log file and to the stream's tap. No reformatting happens here.
func (s *LogStreamImpl) pump(src io.Reader, streamType string, rawTap *tap) {
	defer s.Stop()

	buf := make([]byte, 0, 4096)
	chunk := make([]byte, 4096)
	for {
		n, err := src.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			for {
				i := bytes.IndexByte(buf, '\n')
				if i < 0 {
					if len(buf) < maxLineBytes {
						break
					}
					klog.InfoS("logstream: force-flushing oversized line", "stream", streamType, "buflen", len(buf))
					i = len(buf) - 1
				}
				line := buf[:i+1]
				buf = buf[i+1:]

				// File write — hold the lock so Stop can't close the fd mid-write.
				s.mu.Lock()
				if s.logFile != nil {
					if _, werr := s.logFile.Write(line); werr != nil {
						klog.ErrorS(werr, "logstream: file write failed", "stream", streamType)
					}
				}
				s.mu.Unlock()

				// Tap for external consumers — discards until someone attaches.
				if _, werr := rawTap.Write(line); werr != nil {
					return
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				klog.V(2).InfoS("logstream: pump exiting", "stream", streamType, "error", err)
			}
			return
		}
	}
}

func (s *LogStreamImpl) Stop() {
	if !s.started.Load() {
		return
	}
	// CAS ensures only one caller runs the stop body; the other loses and returns.
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelFn != nil {
		s.cancelFn()
	}
	if s.closeSource != nil {
		s.closeSource()
	}
	if s.logFile != nil {
		s.logFile.Close()
		s.logFile = nil
	}
}

func (s *LogStreamImpl) Destroy() {
	if s.destroyed.Load() {
		return
	}
	s.Stop()
	s.destroyed.Store(true)
	s.stdout.Close()
	s.stderr.Close()
}
