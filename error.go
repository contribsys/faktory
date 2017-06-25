package worq

import "runtime/debug"

type worqError struct {
	Error error
	Stack []byte
}

func internalError(err error) *worqError {
	return &worqError{Error: err, Stack: debug.Stack()}
}
