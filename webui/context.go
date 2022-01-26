package webui

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

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
	Root     string
}

func NewContext(ui *WebUI, req *http.Request, resp http.ResponseWriter) *DefaultContext {
	// set locale via cookie
	localeCookie, _ := req.Cookie("faktory_locale")

	var locale string
	if localeCookie != nil {
		locale = localeCookie.Value
	}

	if locale == "" {
		// fall back to browser language
		locale = localeFromHeader(req.Header.Get("Accept-Language"))
	}

	resp.Header().Set("Content-Language", locale)

	return &DefaultContext{
		Context:  req.Context(),
		webui:    ui,
		request:  req,
		response: resp,
		locale:   locale,
		strings:  translations(locale),
		csrf:     ui.Options.EnableCSRF,
		Root:     req.Header.Get("X-Script-Name"),
	}
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

func translations(locale string) map[string]string {
	strs, ok := locales[locale]
	if strs != nil {
		return strs
	}

	if !ok {
		return nil
	}

	if ok {
		// util.Debugf("Booting the %s locale", locale)
		strs := map[string]string{}
		for _, finder := range AssetLookups {
			content, err := finder(fmt.Sprintf("static/locales/%s.yml", locale))
			if err != nil {
				continue
			}

			scn := bufio.NewScanner(bytes.NewReader(content))
			for scn.Scan() {
				kv := strings.Split(scn.Text(), ":")
				if len(kv) == 2 {
					strs[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
				}
			}
		}
		locales[locale] = strs
		return strs
	}

	panic("Shouldn't get here")
}

func acceptableLanguages(header string) []string {
	langs := []string{}
	pairs := strings.Split(header, ",")
	// we ignore the q weighting and just assume the
	// values are sorted by acceptability
	for idx := range pairs {
		trimmed := strings.Trim(pairs[idx], " ")
		split := strings.Split(trimmed, ";")
		langs = append(langs, strings.ToLower(split[0]))
	}
	return langs
}

func localeFromHeader(value string) string {
	if value == "" {
		return "en"
	}

	langs := acceptableLanguages(value)
	// util.Debugf("A-L: %s %v", value, langs)
	for idx := range langs {
		strs := translations(langs[idx])
		if strs != nil {
			return langs[idx]
		}
	}

	// fallback by checking the language component of any dialect pairs, e.g. "sv-se"
	for idx := range langs {
		pair := strings.Split(langs[idx], "-")
		if len(pair) == 2 {
			baselang := pair[0]
			strs := translations(baselang)
			if strs != nil {
				return baselang
			}
		}
	}

	return "en"
}
