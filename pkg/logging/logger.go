package logging

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ComponentLogger delegates to the global logger with a baked-in component field.
// This ensures it always inherits the root logger's writer configuration.
type ComponentLogger struct {
	component string
}

// Component creates a new ComponentLogger for the given component name.
func Component(name string) ComponentLogger {
	return ComponentLogger{component: name}
}

func (c ComponentLogger) Trace() *zerolog.Event {
	return log.Trace().Str("component", c.component)
}

func (c ComponentLogger) Debug() *zerolog.Event {
	return log.Debug().Str("component", c.component)
}

func (c ComponentLogger) Info() *zerolog.Event {
	return log.Info().Str("component", c.component)
}

func (c ComponentLogger) Warn() *zerolog.Event {
	return log.Warn().Str("component", c.component)
}

func (c ComponentLogger) Error() *zerolog.Event {
	return log.Error().Str("component", c.component)
}
