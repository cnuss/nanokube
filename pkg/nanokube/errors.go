package nanokube

import (
	"errors"
	"fmt"
	"runtime"
	"strings"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type ErrorImpl struct {
	code    int
	err     error
	command []string
}

var _ v1.Error = &ErrorImpl{}

func NewError(err error) v1.Error {
	return &ErrorImpl{code: -1, err: err, command: nil}
}

func (e *ErrorImpl) WithCode(code int) v1.Error {
	e.code = code
	return e
}

func (e *ErrorImpl) WithError(err error) v1.Error {
	if err == nil {
		return e
	}
	if e.err == nil {
		e.err = err
	} else {
		e.err = errors.Join(e.err, err)
	}
	return e
}

func (e *ErrorImpl) WithErrors(errs ...error) v1.Error {
	for _, err := range errs {
		e.WithError(err)
	}
	return e
}

func (e *ErrorImpl) WithCommand(cmd []string) v1.Error {
	e.command = cmd
	return e
}

func (e *ErrorImpl) Error() string {
	return e.String()
}

func (e *ErrorImpl) ExitStatus() int {
	return e.code
}

func (e *ErrorImpl) Exited() bool {
	return e.code != -1
}

func (e *ErrorImpl) String() string {
	if e.command != nil && len(e.command) > 0 {
		if e.err == nil {
			return fmt.Sprintf("command %s exited with code %d", strings.Join(e.command, " "), e.code)
		}
		return fmt.Sprintf("command %s exited with code %d: %v", strings.Join(e.command, " "), e.code, e.err)
	}
	if e.err == nil {
		return fmt.Sprintf("nanokube error: %d", e.code)
	}
	return fmt.Sprintf("nanokube error: %d: %v", e.code, e.err)
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
