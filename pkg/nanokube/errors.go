package nanokube

import (
	"fmt"
	"runtime"
	"strings"
)

type FatalError struct {
	code int
	err  error
}

func NewFatalError(err error) *FatalError {
	return &FatalError{code: 1, err: err}
}

func (e *FatalError) WithCode(code int) *FatalError {
	e.code = code
	return e
}

func (e *FatalError) Error() string {
	return e.err.Error()
}

func (e *FatalError) Code() int {
	return e.code
}

// Unimplemented returns an error that includes the calling function's name.
func Unimplemented() error {
	pc, _, _, _ := runtime.Caller(1)
	name := runtime.FuncForPC(pc).Name()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	panic(fmt.Sprintf("unimplemented: %s", name))
	// return fmt.Errorf("unimplemented: %s", name)
}
