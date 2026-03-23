package component

import (
	"fmt"
	"runtime"
	"strings"
)

// WrapErr wraps an error with the calling function's name and logs it.
// Optional args are logged as "arg0", "arg1", etc. via Any().
func WrapErr(log Logger, err error, args ...any) error {
	pc, _, _, _ := runtime.Caller(1)
	name := runtime.FuncForPC(pc).Name()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	ev := log.Warn().Err(err)
	for i, arg := range args {
		ev = ev.Any(fmt.Sprintf("arg%d", i), arg)
	}
	ev.Msg(name)
	return fmt.Errorf("%s: %w", name, err)
}
