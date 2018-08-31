package webui

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/justinas/nosurf"
)

type Tab struct {
	Name string
	Path string
}

var (
	DefaultTabs = []Tab{
		{"Home", "/"},
		{"Busy", "/busy"},
		{"Queues", "/queues"},
		{"Retries", "/retries"},
		{"Scheduled", "/scheduled"},
		{"Dead", "/morgue"},
	}

	// these are used in testing only
	staticHandler = Cache(http.FileServer(&AssetFS{Asset: Asset, AssetDir: AssetDir}))
)

//go:generate ego .
//go:generate go-bindata -pkg webui -o static.go static/...

type localeMap map[string]map[string]string

var (
	locales = localeMap{}
)

func init() {
	localeFiles, err := AssetDir("static/locales")
	if err != nil {
		panic(err)
	}
	for _, filename := range localeFiles {
		name := strings.Split(filename, ".")[0]
		locales[name] = nil
	}
	translations("en") // eager load English
	//util.Debugf("Initialized %d locales", len(localeFiles))
}

type WebUI struct {
	Options Options
	Server  *server.Server

	mux    *http.ServeMux
	closer func() error
}

type Options struct {
	Binding    string
	Password   string
	EnableCSRF bool
}

func DefaultOptions() Options {
	return Options{
		Password:   "",
		Binding:    ":7420",
		EnableCSRF: true,
	}
}

func NewWeb(s *server.Server, opts Options) *WebUI {
	ui := &WebUI{
		Options: opts,
		Server:  s,

		mux: http.NewServeMux(),
	}

	ui.mux.HandleFunc("/static/", staticHandler)
	ui.mux.HandleFunc("/stats", DebugLog(ui, statsHandler))

	ui.mux.HandleFunc("/", Log(ui, GetOnly(indexHandler)))
	ui.mux.HandleFunc("/queues", Log(ui, queuesHandler))
	ui.mux.HandleFunc("/queues/", Log(ui, queueHandler))
	ui.mux.HandleFunc("/retries", Log(ui, retriesHandler))
	ui.mux.HandleFunc("/retries/", Log(ui, retryHandler))
	ui.mux.HandleFunc("/scheduled", Log(ui, scheduledHandler))
	ui.mux.HandleFunc("/scheduled/", Log(ui, scheduledJobHandler))
	ui.mux.HandleFunc("/morgue", Log(ui, morgueHandler))
	ui.mux.HandleFunc("/morgue/", Log(ui, deadHandler))
	ui.mux.HandleFunc("/busy", Log(ui, busyHandler))
	ui.mux.HandleFunc("/debug", Log(ui, debugHandler))

	return ui
}

func (ui *WebUI) Stop() error {
	return ui.closer()
}

func (ui *WebUI) Run() (func(), error) {
	s := &http.Server{
		Addr:           ui.Options.Binding,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 16,
		Handler:        ui.mux,
	}

	go func() {
		err := s.ListenAndServe()
		if err != http.ErrServerClosed {
			util.Error(fmt.Sprintf("%s server crashed", ui.Options.Binding), err)
		}
	}()
	util.Infof("Web server now listening at %s", ui.Options.Binding)
	return func() { s.Shutdown(context.Background()) }, nil
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
		//util.Debugf("Booting the %s locale", locale)
		content, err := Asset(fmt.Sprintf("static/locales/%s.yml", locale))
		if err != nil {
			panic(err)
		}

		strs := map[string]string{}
		scn := bufio.NewScanner(bytes.NewReader(content))
		for scn.Scan() {
			kv := strings.Split(scn.Text(), ":")
			if len(kv) == 2 {
				strs[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
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
	for _, pair := range pairs {
		trimmed := strings.Trim(pair, " ")
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
	//util.Debugf("A-L: %s %v", value, langs)
	for _, lang := range langs {
		strs := translations(lang)
		if strs != nil {
			return lang
		}
	}

	// fallback by checking the language component of any dialect pairs, e.g. "sv-se"
	for _, lang := range langs {
		pair := strings.Split(lang, "-")
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

/////////////////////////////////////

// The stats handler is hit a lot and adds much noise to the log,
// quiet it down.
func DebugLog(ui *WebUI, pass http.HandlerFunc) http.HandlerFunc {
	return Setup(ui, pass, true)
}

func Log(ui *WebUI, pass http.HandlerFunc) http.HandlerFunc {
	return Protect(ui.Options.EnableCSRF, Setup(ui, pass, false))
}

func Setup(ui *WebUI, pass http.HandlerFunc, debug bool) http.HandlerFunc {
	genericSetup := func(w http.ResponseWriter, r *http.Request) {
		// this is the entry point for every dynamic request
		// static assets bypass all this hubbub
		start := time.Now()

		// negotiate the language to be used for rendering

		// set locale via cookie
		localeCookie, _ := r.Cookie("faktory_locale")

		var locale string
		if localeCookie != nil {
			locale = localeCookie.Value
		}

		if locale == "" {
			// fall back to browser language
			locale = localeFromHeader(r.Header.Get("Accept-Language"))
		}

		w.Header().Set("Content-Language", locale)

		dctx := &DefaultContext{
			Context:  r.Context(),
			webui:    ui,
			response: w,
			request:  r,
			locale:   locale,
			strings:  translations(locale),
			csrf:     ui.Options.EnableCSRF,
		}

		pass(w, r.WithContext(dctx))
		if debug {
			util.Debugf("%s %s %v", r.Method, r.RequestURI, time.Since(start))
		} else {
			util.Infof("%s %s %v", r.Method, r.RequestURI, time.Since(start))
		}
	}
	if ui.Options.Password != "" {
		return BasicAuth(ui.Options.Password, genericSetup)
	}
	return genericSetup
}

func BasicAuth(pwd string, pass http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="Faktory"`)
			http.Error(w, "Authorization required", http.StatusUnauthorized)
			return
		}
		if subtle.ConstantTimeCompare([]byte(password), []byte(pwd)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Faktory"`)
			http.Error(w, "Authorization failed", http.StatusUnauthorized)
			return
		}
		pass(w, r)
	}
}

func GetOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			h(w, r)
			return
		}
		http.Error(w, "get only", http.StatusMethodNotAllowed)
	}
}

func PostOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			h(w, r)
			return
		}
		http.Error(w, "post only", http.StatusMethodNotAllowed)
	}
}

func Cache(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Cache-Control", "public, max-age=3600")
		h.ServeHTTP(w, r)
	}
}

func Protect(enabled bool, h http.HandlerFunc) http.HandlerFunc {
	hndlr := nosurf.New(h)
	hndlr.ExemptFunc(func(r *http.Request) bool {
		return !enabled
	})
	return func(w http.ResponseWriter, r *http.Request) {
		hndlr.ServeHTTP(w, r)
	}
}
