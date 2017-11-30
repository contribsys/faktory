package server

import "fmt"

type taggedError struct {
	Code string
	Err  error
}

func (re *taggedError) Error() string {
	return fmt.Sprintf("%s %s", re.Code, re.Err.Error())
}

func newTaggedError(code string, err error) *taggedError {
	return &taggedError{Code: code, Err: err}
}
