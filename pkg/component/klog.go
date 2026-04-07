package component

import (
	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
	"k8s.io/klog/v2"
)

// SetupKlog routes all klog output through zerolog.
func SetupKlog() {
	klog.SetLogger(logr.New(&zerologSink{log: &rootLog}))
}

// zerologSink implements logr.LogSink backed by a zerolog.Logger.
type zerologSink struct {
	log    *zerolog.Logger
	name   string
	values []any
}

func (s *zerologSink) Init(logr.RuntimeInfo) {}
func (s *zerologSink) Enabled(level int) bool {
	return s.log.GetLevel() <= klogToZerolog(level)
}

func (s *zerologSink) Info(level int, msg string, keysAndValues ...any) {
	evt := s.log.WithLevel(klogToZerolog(level))
	if s.name != "" {
		evt = evt.Str("component", s.name)
	}
	addFields(evt, s.values)
	addFields(evt, keysAndValues)
	evt.Msg(msg)
}

func (s *zerologSink) Error(err error, msg string, keysAndValues ...any) {
	evt := s.log.Error().Err(err)
	if s.name != "" {
		evt = evt.Str("component", s.name)
	}
	addFields(evt, s.values)
	addFields(evt, keysAndValues)
	evt.Msg(msg)
}

func (s *zerologSink) WithValues(keysAndValues ...any) logr.LogSink {
	return &zerologSink{log: s.log, name: s.name, values: append(s.values, keysAndValues...)}
}

func (s *zerologSink) WithName(name string) logr.LogSink {
	n := name
	if s.name != "" {
		n = s.name + "." + name
	}
	return &zerologSink{log: s.log, name: n, values: s.values}
}

// klogToZerolog maps klog verbosity (0=info, higher=more verbose) to zerolog levels.
func klogToZerolog(level int) zerolog.Level {
	switch {
	case level <= 0:
		return zerolog.InfoLevel
	case level <= 2:
		return zerolog.DebugLevel
	default:
		return zerolog.TraceLevel
	}
}

func addFields(evt *zerolog.Event, keysAndValues []any) {
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		evt.Interface(key, keysAndValues[i+1])
	}
}
