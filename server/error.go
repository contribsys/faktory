package server

import "runtime/debug"

type runtimeError struct {
	Error error
	Stack []byte
}

func internalError(err error) *runtimeError {
	return &runtimeError{Error: err, Stack: debug.Stack()}
}
