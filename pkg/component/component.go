package component

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var rootLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

// Component is the lifecycle interface for nanokube subsystems.
type Component interface {
	Start(ctx context.Context) error
	Stop()
}

// Logger delegates to the package-level root logger with a baked-in component field.
type Logger struct {
	component string
}

// Setup parses flags from the cobra command, applies the log level,
// sets up log file mirroring, and returns the command, root logger, and cleanup func.
func Setup(cmd *cobra.Command) (*cobra.Command, zerolog.Logger, func()) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	cmd.ParseFlags(os.Args[1:])

	v, _ := cmd.Flags().GetCount("verbose")
	applyLogLevel(v)

	dataDir, _ := cmd.Flags().GetString("data")
	name, _ := cmd.Flags().GetString("name")
	clean, _ := cmd.Flags().GetBool("clean")

	if clean {
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0755)

	// Set up log file mirroring — all output (zerolog + klog + etcd) goes to disk
	logFile, err := os.OpenFile(filepath.Join(dataDir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return cmd, rootLog, func() {}
	}

	multi := zerolog.MultiLevelWriter(
		zerolog.ConsoleWriter{Out: os.Stderr},
		logFile,
	)
	rootLog = zerolog.New(multi).With().Timestamp().Logger()
	os.Stderr = teeFile(os.Stderr, logFile)
	os.Stdout = teeFile(os.Stdout, logFile)

	return cmd, rootLog, func() { logFile.Close() }
}

func applyLogLevel(verbosity int) {
	switch {
	case verbosity >= 2:
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case verbosity >= 1:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

// NewLogger creates a Logger for the given component name.
func NewLogger(name string) Logger {
	return Logger{component: name}
}

func (c Logger) Trace() *zerolog.Event {
	return rootLog.Trace().Str("component", c.component)
}

func (c Logger) Debug() *zerolog.Event {
	return rootLog.Debug().Str("component", c.component)
}

func (c Logger) Info() *zerolog.Event {
	return rootLog.Info().Str("component", c.component)
}

func (c Logger) Warn() *zerolog.Event {
	return rootLog.Warn().Str("component", c.component)
}

func (c Logger) Error() *zerolog.Event {
	return rootLog.Error().Str("component", c.component)
}

// teeFile creates a pipe that copies writes to both orig and logFile.
func teeFile(orig *os.File, logFile *os.File) *os.File {
	r, w, err := os.Pipe()
	if err != nil {
		return orig
	}
	go func() {
		mw := io.MultiWriter(orig, logFile)
		io.Copy(mw, r)
	}()
	return w
}
