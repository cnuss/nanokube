package pkg

import (
	"os"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
)

type zerologSink struct {
	log    *zerolog.Logger
	name   string
	values []any
}

var logrLevels = []zerolog.Level{
	zerolog.WarnLevel,
	zerolog.InfoLevel,
	zerolog.DebugLevel,
	zerolog.TraceLevel,
}

func toZerologLevel(level int) zerolog.Level {
	if level >= len(logrLevels) {
		return logrLevels[len(logrLevels)-1]
	}
	return logrLevels[level]
}

func (z *zerologSink) Init(info logr.RuntimeInfo) {}

func (z *zerologSink) Enabled(level int) bool {
	return z.log.GetLevel() <= toZerologLevel(level)
}

func (z *zerologSink) Info(level int, msg string, keysAndValues ...any) {
	e := z.log.WithLevel(toZerologLevel(level))
	z.addFields(e, keysAndValues...).Msg(msg)
}

func (z *zerologSink) Error(err error, msg string, keysAndValues ...any) {
	e := z.log.Error().Err(err)
	z.addFields(e, keysAndValues...).Msg(msg)
}

func (z *zerologSink) WithName(name string) logr.LogSink {
	n := z.name
	if n != "" {
		n += "."
	}
	n += name
	sub := z.log.With().Str("logger", n).Logger()
	return &zerologSink{log: &sub, name: n, values: z.values}
}

func (z *zerologSink) WithValues(keysAndValues ...any) logr.LogSink {
	vals := make([]any, len(z.values)+len(keysAndValues))
	copy(vals, z.values)
	copy(vals[len(z.values):], keysAndValues)
	return &zerologSink{log: z.log, name: z.name, values: vals}
}

func (z *zerologSink) addFields(e *zerolog.Event, keysAndValues ...any) *zerolog.Event {
	all := append(z.values, keysAndValues...)
	for i := 0; i+1 < len(all); i += 2 {
		key, ok := all[i].(string)
		if !ok {
			key = "INVALID_KEY"
		}
		e = e.Interface(key, all[i+1])
	}
	return e
}

var _ logr.LogSink = &zerologSink{}

// levelWriter routes log output by level: everything goes to the logfile,
// Info is mirrored to stdout, Error+ is mirrored to stderr.
type levelWriter struct {
	file *os.File
}

func (w *levelWriter) Write(p []byte) (n int, err error) {
	return w.file.Write(p)
}

func (w *levelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	n, err = w.file.Write(p)
	if err != nil {
		return n, err
	}
	switch level {
	case zerolog.InfoLevel:
		zerolog.ConsoleWriter{Out: os.Stdout}.Write(p)
	case zerolog.WarnLevel, zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		zerolog.ConsoleWriter{Out: os.Stderr}.Write(p)
	}
	return n, nil
}
