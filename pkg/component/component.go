package component

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Component is the lifecycle interface for nanokube subsystems.
type Component interface {
	Start(ctx context.Context) error
}

// Logger delegates to the global zerolog logger with a baked-in component field.
// This ensures it always inherits the root logger's writer configuration.
type Logger struct {
	component string
}

// NewLogger creates a Logger for the given component name.
func NewLogger(name string) Logger {
	return Logger{component: name}
}

func (c Logger) Trace() *zerolog.Event {
	return log.Trace().Str("component", c.component)
}

func (c Logger) Debug() *zerolog.Event {
	return log.Debug().Str("component", c.component)
}

func (c Logger) Info() *zerolog.Event {
	return log.Info().Str("component", c.component)
}

func (c Logger) Warn() *zerolog.Event {
	return log.Warn().Str("component", c.component)
}

func (c Logger) Error() *zerolog.Event {
	return log.Error().Str("component", c.component)
}
