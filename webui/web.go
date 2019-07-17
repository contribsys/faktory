package webui

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"io"
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
	staticHandler = cache(http.FileServer(&AssetFS{Asset: Asset, AssetDir: AssetDir}))
)

//go:generate ego .
//go:generate go-bindata -pkg webui -o static.go static/...

type localeMap map[string]map[string]string
type assetLookup func(string) ([]byte, error)

var (
	AssetLookups = []assetLookup{Asset}
	locales      = localeMap{}
)

func init() {
	files, err := AssetDir("static/locales")
	if err != nil {
		panic(err)
	}
	for _, filename := range files {
		name := strings.Split(filename, ".")[0]
		locales[name] = nil
	}
	//util.Debugf("Initialized %d locales", len(files))
}

type Lifecycle struct {
	WebUI          *WebUI
	defaultBinding string
	closer         func()
}

func Subsystem(binding string) *Lifecycle {
	return &Lifecycle{
		defaultBinding: binding,
	}
}

type WebUI struct {
	Options Options
	Server  *server.Server
	Mux     *http.ServeMux
}

type Options struct {
	Binding    string
	Password   string
	EnableCSRF bool
}

func defaultOptions() Options {
	return Options{
		Password:   "",
		Binding:    "localhost:7420",
		EnableCSRF: true,
	}
}

func newWeb(s *server.Server, opts Options) *WebUI {
	ui := &WebUI{
		Options: opts,
		Server:  s,

		Mux: http.NewServeMux(),
	}

	app := http.NewServeMux()
	app.HandleFunc("/static/", staticHandler)
	app.HandleFunc("/stats", DebugLog(ui, statsHandler))

	app.HandleFunc("/", Log(ui, GetOnly(indexHandler)))
	app.HandleFunc("/queues", Log(ui, queuesHandler))
	app.HandleFunc("/queues/", Log(ui, queueHandler))
	app.HandleFunc("/retries", Log(ui, retriesHandler))
	app.HandleFunc("/retries/", Log(ui, retryHandler))
	app.HandleFunc("/scheduled", Log(ui, scheduledHandler))
	app.HandleFunc("/scheduled/", Log(ui, scheduledJobHandler))
	app.HandleFunc("/morgue", Log(ui, morgueHandler))
	app.HandleFunc("/morgue/", Log(ui, deadHandler))
	app.HandleFunc("/busy", Log(ui, busyHandler))
	app.HandleFunc("/debug", Log(ui, debugHandler))

	ui.Mux.HandleFunc("/", Proxy(ui, app))

	return ui
}

func Proxy(ui *WebUI, app *http.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		///////
		// Support transparent proxying with nginx's proxy_pass.
		// Note that's super critical that location == X-Script-Name
		// Example config:
		/*
		   location /faktory {
		       proxy_set_header X-Script-Name /faktory;

		       proxy_pass   http://127.0.0.1:7420;
		       proxy_set_header Host $host;
		       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
		       proxy_set_header X-Scheme $scheme;
		       proxy_set_header X-Real-IP $remote_addr;
		   }
		*/

		prefix := r.Header.Get("X-Script-Name")
		if prefix != "" {
			r.RequestURI = strings.Replace(r.RequestURI, prefix, "", 1)
			r.URL.Path = r.RequestURI
		}
		app.ServeHTTP(w, r)
	}
}

func (l *Lifecycle) opts(s *server.Server) Options {
	opts := defaultOptions()
	opts.Binding = l.defaultBinding
	if opts.Binding == "localhost:7420" {
		opts.Binding = s.Options.String("web", "binding", "localhost:7420")
	}
	// Allow the Web UI to have a different password from the command port
	// so you can rotate user-used passwords and machine-used passwords separately
	pwd := s.Options.String("web", "password", "")
	if pwd == "" {
		pwd = s.Options.Password
	}
	opts.Password = pwd
	return opts
}

func (l *Lifecycle) Start(s *server.Server) error {
	uiopts := l.opts(s)

	l.WebUI = newWeb(s, uiopts)
	closer, err := l.WebUI.Run()
	if err != nil {
		return err
	}
	l.closer = closer
	return nil
}

func (l *Lifecycle) Name() string {
	return "Web UI"
}

func (l *Lifecycle) Reload(s *server.Server) error {
	uiopts := l.opts(s)

	if uiopts != l.WebUI.Options {
		util.Infof("Reloading web interface")
		l.closer()

		l.WebUI.Options = uiopts
		closer, err := l.WebUI.Run()
		if err != nil {
			return err
		}
		l.closer = closer
		return nil
	}
	return nil
}

func (l *Lifecycle) Shutdown(s *server.Server) error {
	if l.closer != nil {
		util.Debug("Stopping WebUI")
		l.closer()
		l.closer = nil
		l.WebUI = nil
	}
	return nil
}

func (ui *WebUI) Run() (func(), error) {
	if ui.Options.Binding == ":0" {
		// disable webui
		return nil, nil
	}

	s := &http.Server{
		Addr:           ui.Options.Binding,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 16,
		Handler:        ui.Mux,
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

func Layout(w io.Writer, req *http.Request, yield func()) {
	ego_layout(w, req, yield)
}

/////////////////////////////////////

// The stats handler is hit a lot and adds much noise to the log,
// quiet it down.
func DebugLog(ui *WebUI, pass http.HandlerFunc) http.HandlerFunc {
	return setup(ui, pass, true)
}

func Log(ui *WebUI, pass http.HandlerFunc) http.HandlerFunc {
	return protect(ui.Options.EnableCSRF, setup(ui, pass, false))
}

func setup(ui *WebUI, pass http.HandlerFunc, debug bool) http.HandlerFunc {
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
			root:     r.Header.Get("X-Script-Name"),
		}

		pass(w, r.WithContext(dctx))
		if debug {
			util.Debugf("%s %s %v", r.Method, r.RequestURI, time.Since(start))
		} else {
			util.Infof("%s %s %v", r.Method, r.RequestURI, time.Since(start))
		}
	}
	if ui.Options.Password != "" {
		return basicAuth(ui.Options.Password, genericSetup)
	}
	return genericSetup
}

func basicAuth(pwd string, pass http.HandlerFunc) http.HandlerFunc {
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

func cache(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Cache-Control", "public, max-age=3600")
		h.ServeHTTP(w, r)
	}
}

func protect(enabled bool, h http.HandlerFunc) http.HandlerFunc {
	hndlr := nosurf.New(h)
	hndlr.ExemptFunc(func(r *http.Request) bool {
		return !enabled
	})
	return func(w http.ResponseWriter, r *http.Request) {
		hndlr.ServeHTTP(w, r)
	}
}
