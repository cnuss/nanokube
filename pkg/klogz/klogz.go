// Package klogz routes all klog output through a single zerolog logger and
// demotes unconditional (non-V) logs that originate outside nanokube to V(1),
// so that at the default verbosity only nanokube's own logs and foreign errors
// are shown.
//
// It plugs in as a component-base log format ("nanokube") rather than calling
// klog.SetLogger directly, so component-base installs the logger through its
// own apply path. That avoids the last-writer-wins clobber that happens when a
// component's logsapi.Apply runs after a manual SetLogger.
package klogz

import (
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
)

const (
	// FormatName is the --logging-format value that selects this logger.
	FormatName = "nanokube"
	// modulePrefix matches function names compiled from nanokube's own code.
	modulePrefix = "github.com/cnuss/nanokube"
	// shimPrefix matches this package's frames, which must be skipped when
	// attributing a log line to its real caller.
	shimPrefix = "github.com/cnuss/nanokube/pkg/klogz"
)

// factory builds the zerolog-backed logger for the "nanokube" log format.
type factory struct{}

func (factory) Create(c logsapi.LoggingConfiguration, o logsapi.LoggingOptions) (logr.Logger, logsapi.RuntimeControl) {
	var out io.Writer = os.Stderr
	if o.ErrorStream != nil {
		out = o.ErrorStream
	}
	zl := zerolog.New(zerolog.ConsoleWriter{
		Out:        out,
		TimeFormat: zerolog.TimeFormatUnix,
		NoColor:    true,
	}).With().Timestamp().Logger()
	return logr.New(&sink{logger: zl}), logsapi.RuntimeControl{}
}

// Register makes the "nanokube" log format available to component-base. Call it
// once at process start, before any component applies its logging config.
func Register() error {
	return logsapi.RegisterLogFormat(FormatName, factory{}, logsapi.LoggingStableOptions)
}

// fromNanokube reports whether the first stack frame outside the logging
// plumbing (this package, klog, go-logr) belongs to nanokube. It scans frames
// rather than using a fixed skip so it survives changes in klog/logr call depth.
func fromNanokube() bool {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		fn := f.Function
		switch {
		case strings.HasPrefix(fn, shimPrefix),
			strings.HasPrefix(fn, "k8s.io/klog"),
			strings.HasPrefix(fn, "github.com/go-logr"):
			// logging plumbing; keep scanning for the real caller
		case strings.HasPrefix(fn, modulePrefix):
			return true
		default:
			return false
		}
		if !more {
			return false
		}
	}
}

// sink is a logr.LogSink backed by zerolog. Verbosity gating is delegated to
// klog's own -v via klog.V (which reads klog's verbosity directly, not the
// installed logger, so there is no recursion).
type sink struct {
	logger zerolog.Logger
	name   string
}

func (s *sink) Init(logr.RuntimeInfo) {}

func (s *sink) Enabled(level int) bool {
	return klog.V(klog.Level(level)).Enabled()
}

func (s *sink) Info(level int, msg string, kv ...any) {
	// Unconditional foreign logs (level 0, caller outside nanokube) are demoted
	// to V(1) so they are hidden at the default verbosity.
	if level == 0 && !fromNanokube() {
		return
	}
	if !s.Enabled(level) {
		return
	}
	var e *zerolog.Event
	switch {
	case level <= 0:
		e = s.logger.Info()
	case level == 1:
		e = s.logger.Debug()
	default:
		e = s.logger.Trace()
	}
	s.emit(e, msg, kv)
}

// Error is always emitted regardless of caller, so foreign errors stay visible.
func (s *sink) Error(err error, msg string, kv ...any) {
	e := s.logger.Error().Err(err)
	s.emit(e, msg, kv)
}

func (s *sink) emit(e *zerolog.Event, msg string, kv []any) {
	if s.name != "" {
		e = e.Str("logger", s.name)
	}
	if len(kv) > 0 {
		e = e.Fields(kv)
	}
	e.Msg(msg)
}

func (s *sink) WithValues(kv ...any) logr.LogSink {
	return &sink{logger: s.logger.With().Fields(kv).Logger(), name: s.name}
}

func (s *sink) WithName(name string) logr.LogSink {
	if s.name != "" {
		name = s.name + "." + name
	}
	return &sink{logger: s.logger, name: name}
}
