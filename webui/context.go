package webui

import (
	"context"
	"net/http"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
)

type Context interface {
	context.Context

	Request() string
	Response() string
}

type DefaultContext struct {
	context.Context

	webui    *WebUI
	response http.ResponseWriter
	request  *http.Request
	locale   string
	strings  map[string]string
	csrf     bool
}

func (d *DefaultContext) Response() http.ResponseWriter {
	return d.response
}

func (d *DefaultContext) Request() *http.Request {
	return d.request
}

func (d *DefaultContext) UseCsrf() bool {
	return d.csrf
}

func (d *DefaultContext) Store() storage.Store {
	return d.webui.Server.Store()
}

func (d *DefaultContext) Server() *server.Server {
	return d.webui.Server
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
