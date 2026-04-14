package nanokube

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/lmittmann/tint"
	"k8s.io/klog/v2"
)

// Log is nanokube's own logger — always visible regardless of -v.
var Log = slog.New(tint.NewHandler(os.Stderr, &tint.Options{
	Level:      slog.LevelInfo,
	TimeFormat: time.TimeOnly,
}))

// SetupLogging configures klog to output through a colorized handler.
// Verbosity controls what's visible:
//
//	0: errors only (quiet)
//	1: + warnings, info, klog V(0-2)
//	2: + klog V(3-4)
//	3: + klog V(5-6)
//	4+: + klog V(7-8), everything
func SetupLogging(verbosity int) {
	var level slog.Level
	switch {
	case verbosity >= 4:
		level = slog.Level(-8)
	case verbosity >= 3:
		level = slog.Level(-6)
	case verbosity >= 2:
		level = slog.Level(-4)
	case verbosity >= 1:
		level = slog.Level(-2)
	default:
		level = slog.LevelError
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
