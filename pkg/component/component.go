package component

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var rootLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

type Started <-chan struct{}
type Stopped <-chan struct{}

// Component is the lifecycle interface for nanokube subsystems.
type Component interface {
	Start(ctx context.Context) (Started, error)
	Stop() Stopped
}

// HostnameProvider returns the node hostname for this runtime.
type HostnameProvider interface {
	Hostname() string
}

// Logger delegates to the package-level root logger with a baked-in component field.
type Logger struct {
	component string
}

// Setup parses flags from the cobra command, applies the log level,
// sets up log file mirroring, and returns the command, root logger, and cleanup func.
func Setup(cmd *cobra.Command) (*cobra.Command, zerolog.Logger, func()) {
	zerolog.TimeFieldFormat = time.RFC3339
	cmd.ParseFlags(os.Args[1:])

	v, _ := cmd.Flags().GetCount("verbose")
	applyLogLevel(v)

	dataDir, _ := cmd.Flags().GetString("data")
	name, _ := cmd.Flags().GetString("name")
	clean, _ := cmd.Flags().GetBool("clean")

	if dataDir == "" {
		home, _ := os.UserHomeDir()
		dataDir = filepath.Join(home, "."+name)
	}

	if clean {
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0755)

	// Set up log file mirroring — all output (zerolog + klog + etcd) goes to disk
	logFile, err := os.OpenFile(filepath.Join(dataDir, "log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return cmd, rootLog, func() {}
	}

	multi := zerolog.MultiLevelWriter(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
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

// Ready returns an already-closed Started channel for components that
// block in Start() until healthy.
func Ready() Started {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Done returns an already-closed Stopped channel for components that
// need no shutdown work.
func Done() Stopped {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Opened runs setup (if non-nil), then polls until the network endpoint
// starts accepting connections. Returns a Started channel that closes when ready.
func Opened(network, address string, setup func()) Started {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		if setup != nil {
			setup()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := net.DialTimeout(network, address, 200*time.Millisecond)
				if err == nil {
					conn.Close()
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return ch
}

// Closed runs shutdown (if non-nil), then signals done immediately.
func Closed(network, address string, shutdown func()) Stopped {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if shutdown != nil {
			shutdown()
		}
	}()
	return done
}

func NotReady(shutdown func()) Stopped {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if shutdown != nil {
			shutdown()
		}
	}()
	return done
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
