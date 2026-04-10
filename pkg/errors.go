package pkg

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
