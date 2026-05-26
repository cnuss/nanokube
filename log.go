package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/go-logr/logr"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
)

const logFormatName = "nanokube"

// PrettyLogging is true when our pretty format is active. Toggle off via
// NANOKUBE_PRETTY=0 to see raw klog output for side-by-side comparison.
var PrettyLogging = os.Getenv("NANOKUBE_PRETTY") != "0"

// knownComponents drives prefix padding and color stability.
var knownComponents = []string{
	"nanokube",
	"apiserver",
	"controller",
	"scheduler",
	"kubelet",
	"etcd",
	"?",
}

// prefixWidth is the rendered "[name]" column width, padded to the longest
// known component plus brackets.
var prefixWidth = func() int {
	w := 0
	for _, n := range knownComponents {
		if l := len(n) + 2; l > w {
			w = l
		}
	}
	return w
}()

func setupLogging(cancel context.CancelCauseFunc) {
	klog.OsExit = func(code int) {
		cancel(fmt.Errorf("exiting with code %d", code))
	}
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
	if !PrettyLogging {
		return
	}
	if err := logsapi.RegisterLogFormat(logFormatName, prettyFormat{}, logsapi.LoggingStableOptions); err != nil {
		panic(err)
	}
	klog.SetSlogLogger(slog.New(newPrettyHandler(os.Stderr)))
}

type prettyFormat struct{}

func (prettyFormat) Create(c logsapi.LoggingConfiguration, o logsapi.LoggingOptions) (logr.Logger, logsapi.RuntimeControl) {
	out := io.Writer(os.Stderr)
	if o.ErrorStream != nil {
		out = o.ErrorStream
	}
	return logr.FromSlogHandler(newPrettyHandler(out)), logsapi.RuntimeControl{}
}

func newPrettyHandler(out io.Writer) slog.Handler {
	return &prettyHandler{out: out, mu: &sync.Mutex{}}
}

// prettyHandler renders records as:
//
//	HH:MM:SS LVL [component] file.go:123 message  key=val key2=val2
//
// Component is detected via stack walk; tint-style coloring (level, faint
// time/source, cyan keys, red errors) is reproduced inline.
type prettyHandler struct {
	out    io.Writer
	mu     *sync.Mutex
	attrs  []slog.Attr
	groups []string
}

func (h *prettyHandler) Enabled(_ context.Context, l slog.Level) bool {
	// Info/Warn/Error always pass. For V(N) records (negative slog levels,
	// where V(8) = slog.Level(-8)), gate against klog's own verbosity so
	// that logr-based code paths like `klog.FromContext(ctx).V(8).Info(…)`
	// — which bypass klog's V() check — still honor --v.
	if l >= slog.LevelInfo {
		return true
	}
	return klog.V(klog.Level(-int(l))).Enabled()
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	component := detectComponent()

	var b strings.Builder

	// Time (faint)
	b.WriteString(faint(r.Time.Format(time.TimeOnly)))
	b.WriteByte(' ')

	// Level (colored)
	b.WriteString(levelTag(r.Level))
	b.WriteByte(' ')

	// Component prefix (padded for column alignment)
	label := fmt.Sprintf("[%s]", component)
	b.WriteString(colorize(component, label))
	if pad := prefixWidth - len(label); pad > 0 {
		b.WriteString(strings.Repeat(" ", pad))
	}
	b.WriteByte(' ')

	// Source (faint) — just the base filename, without `.go` or line number.
	if r.PC != 0 {
		if fn := runtime.FuncForPC(r.PC); fn != nil {
			file, _ := fn.FileLine(r.PC)
			b.WriteString(faint(strings.TrimSuffix(filepath.Base(file), ".go")))
			b.WriteByte(' ')
		}
	}

	// Message
	b.WriteString(r.Message)

	// Attrs (preset + per-record)
	for _, a := range h.attrs {
		writeAttr(&b, a, h.groups)
	}
	r.Attrs(func(a slog.Attr) bool {
		writeAttr(&b, a, h.groups)
		return true
	})

	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.out, b.String())
	return err
}

func (h *prettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &n
}

func (h *prettyHandler) WithGroup(name string) slog.Handler {
	n := *h
	n.groups = append(append([]string{}, h.groups...), name)
	return &n
}

func writeAttr(b *strings.Builder, a slog.Attr, groups []string) {
	if a.Equal(slog.Attr{}) {
		return
	}
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		sub := append(groups[:len(groups):len(groups)], a.Key)
		for _, child := range a.Value.Group() {
			writeAttr(b, child, sub)
		}
		return
	}

	b.WriteByte(' ')

	// key (cyan) with group prefix
	key := a.Key
	if len(groups) > 0 {
		key = strings.Join(groups, ".") + "." + a.Key
	}
	b.WriteString(cyan(key))
	b.WriteByte('=')

	// value (red if error, plain otherwise; quoted when it has whitespace)
	if err, ok := a.Value.Any().(error); ok {
		b.WriteString(errColor(quoteIfNeeded(err.Error())))
		return
	}
	b.WriteString(quoteIfNeeded(fmt.Sprintf("%v", a.Value.Any())))
}

func quoteIfNeeded(s string) string {
	if strings.ContainsAny(s, " \t\"\n") {
		return strconv.Quote(s)
	}
	return s
}

// componentRule maps a Go package import path to a component label. A frame's
// fully-qualified function name (e.g. "k8s.io/kubernetes/pkg/kubelet.
// (*Kubelet).syncLoop") matches when it starts with pkg followed by either '/'
// (subpackage) or '.' (this exact package). Listed in priority order — more
// specific paths must come before broader ones.
type componentRule struct {
	pkg  string
	name string
}

var componentRules = []componentRule{
	{"github.com/cnuss/nanokube", "nanokube"},
	{"k8s.io/kubernetes/cmd/kube-apiserver", "apiserver"},
	{"k8s.io/apiserver", "apiserver"},
	{"k8s.io/kube-aggregator", "apiserver"},
	{"k8s.io/apiextensions-apiserver", "apiserver"},
	{"k8s.io/kubernetes/pkg/controlplane", "apiserver"},
	{"k8s.io/kubernetes/cmd/kube-controller-manager", "controller"},
	{"k8s.io/kubernetes/pkg/controller", "controller"},
	{"k8s.io/kubernetes/cmd/kube-scheduler", "scheduler"},
	{"k8s.io/kubernetes/pkg/scheduler", "scheduler"},
	{"k8s.io/kubernetes/cmd/kubelet", "kubelet"},
	{"k8s.io/kubernetes/pkg/kubelet", "kubelet"},
	{"go.etcd.io/etcd", "etcd"},
}

func detectComponent() string {
	var pcs [64]uintptr
	n := runtime.Callers(0, pcs[:])
	if n == 0 {
		return "?"
	}
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if c := componentFromFunc(frame.Function); c != "" {
			return c
		}
		if !more {
			break
		}
	}
	return "?"
}

func componentFromFunc(fn string) string {
	for _, r := range componentRules {
		if !strings.HasPrefix(fn, r.pkg) {
			continue
		}
		switch fn[len(r.pkg)] {
		case '/', '.':
			return r.name
		}
	}
	return ""
}

// --- color helpers (fatih/color handles NO_COLOR / non-TTY autodetect) ---

func levelTag(l slog.Level) string {
	// Width matched to V(N) (4 chars) so the column after stays aligned.
	switch {
	case l >= slog.LevelError:
		return color.New(color.FgHiRed).Sprint("ERR ")
	case l >= slog.LevelWarn:
		return color.New(color.FgHiYellow).Sprint("WRN ")
	case l >= slog.LevelInfo:
		return color.New(color.FgHiGreen).Sprint("INF ")
	default:
		return color.New(color.Faint).Sprintf("V(%d)", -int(l))
	}
}

func faint(s string) string { return color.New(color.Faint).Sprint(s) }
func cyan(s string) string  { return color.New(color.FgCyan).Sprint(s) }
func errColor(s string) string {
	return color.New(color.FgHiRed).Sprint(s)
}

// colorize wraps s with a stable 256-color SGR keyed on name. "?" gets neutral
// grey.
func colorize(name, s string) string {
	if name == "?" {
		return color.New(color.Attribute(38), color.Attribute(5), color.Attribute(244)).Sprint(s)
	}
	palette := []int{33, 39, 45, 51, 81, 99, 105, 111, 141, 147, 159, 177, 183, 213}
	h := fnv.New32a()
	h.Write([]byte(name))
	code := palette[int(h.Sum32())%len(palette)]
	return color.New(color.Attribute(38), color.Attribute(5), color.Attribute(code)).Sprint(s)
}
