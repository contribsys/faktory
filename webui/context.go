package webui

import (
	"context"
	"net/http"
)

type Context interface {
	context.Context

	Request() string
	Response() string
}

type DefaultContext struct {
	context.Context

	response http.ResponseWriter
	request  *http.Request
	locale   string
	strings  map[string]string
}

func (d *DefaultContext) Response() http.ResponseWriter {
	return d.response
}

func (d *DefaultContext) Request() *http.Request {
	return d.request
}

type Translator interface {
	Locale() string
	Translation(string) string
}

func (d *DefaultContext) Locale() string {
	return d.locale
}

func (d *DefaultContext) Translation(str string) string {
	val, ok := d.strings[str]
	if ok {
		return val
	}
	return str
}
