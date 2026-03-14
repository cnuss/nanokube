package component

import (
	"fmt"
	"runtime"
	"strings"
)

// WrapErr wraps an error with the calling function's name and logs it.
func WrapErr(log Logger, err error) error {
	pc, _, _, _ := runtime.Caller(1)
	name := runtime.FuncForPC(pc).Name()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	log.Error().Err(err).Msg(name)
	return fmt.Errorf("%s: %w", name, err)
}
