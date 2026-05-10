package nanokube

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lmittmann/tint"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

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
			Log.Error("logstream: failed to create log dir", "path", s.status.LogPath, "error", err)
			s.mu.Unlock()
			s.Stop()
			return
		}
		f, err := os.OpenFile(s.status.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			Log.Error("logstream: failed to open log file", "path", s.status.LogPath, "error", err)
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
		Log.Error("logstream: source failed", "error", err)
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
					Log.Warn("logstream: force-flushing oversized line", "stream", streamType, "buflen", len(buf))
					i = len(buf) - 1
				}
				line := buf[:i+1]
				buf = buf[i+1:]

				// File write — hold the lock so Stop can't close the fd mid-write.
				s.mu.Lock()
				if s.logFile != nil {
					if _, werr := s.logFile.Write(line); werr != nil {
						Log.Warn("logstream: file write failed", "stream", streamType, "error", werr)
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
				Log.Debug("logstream: pump exiting", "stream", streamType, "error", err)
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

// Log is nanokube's own logger — always visible regardless of -v.
var Log = slog.New(tint.NewHandler(os.Stderr, &tint.Options{
	Level:      slog.LevelInfo,
	TimeFormat: time.TimeOnly,
}))

// SetupLogging configures klog to output through a colorized handler.
// Verbosity controls what's visible:
//
//	0: klog silenced entirely
//	1: warnings, info, klog V(0-2)
//	2: + klog V(3-4)
//	3: + klog V(5-6)
//	4+: + klog V(7-8), everything
func SetupLogging(verbosity int) {
	if verbosity < 1 {
		klog.SetSlogLogger(slog.New(slog.DiscardHandler))
		klog.SetOutput(io.Discard)
		return
	}

	var level slog.Level
	switch {
	case verbosity >= 4:
		level = slog.Level(-8)
	case verbosity >= 3:
		level = slog.Level(-6)
	case verbosity >= 2:
		level = slog.Level(-4)
	default:
		level = slog.Level(-2)
	}

	klog.SetSlogLogger(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      level,
		TimeFormat: time.TimeOnly,
		AddSource:  true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				l := a.Value.Any().(slog.Level)
				switch {
				case l >= slog.LevelError:
					a.Value = slog.StringValue("ERR")
				case l >= slog.LevelWarn:
					a.Value = slog.StringValue("WRN")
				case l >= slog.LevelInfo:
					a.Value = slog.StringValue("INF")
				default:
					a.Value = slog.StringValue(fmt.Sprintf("V(%d)", -int(l)))
				}
			}
			if a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok {
					a.Value = slog.StringValue(fmt.Sprintf("%s:%d", filepath.Base(src.File), src.Line))
				}
			}
			return a
		},
	})))
}
