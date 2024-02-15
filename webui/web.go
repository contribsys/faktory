package webui

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/justinas/nosurf"
)

//go:generate ego .

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

	//go:embed static/*.css static/*.js static/img/*
	staticFiles embed.FS

	//go:embed static/locales/*
	localeFiles embed.FS

	staticHandler = cache(http.FileServer(http.FS(staticFiles)))

	LicenseStatus = func(w io.Writer, req *http.Request) string {
		return ""
	}
)

type localeMap map[string]map[string]string
type assetLookup func(string) ([]byte, error)

var (
	AssetLookups = []assetLookup{
		localeFiles.ReadFile,
	}
	locales = localeMap{}
)

func init() {
	files, err := localeFiles.ReadDir("static/locales")
	if err != nil {
		panic(err)
	}
	for idx := range files {
		name := strings.Split(files[idx].Name(), ".")[0]
		locales[name] = nil
	}
	// util.Debugf("Initialized %d locales", len(files))
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
	Options     Options
	Server      *server.Server
	App         *http.ServeMux
	Title       string
	ExtraCssUrl string

	proxy *http.ServeMux
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
		Title:   client.Name,
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
	app.HandleFunc("/health", healthHandler(ui))

	// app.HandleFunc("/debug/pprof/", pprof.Index)
	// app.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	// app.HandleFunc("/debug/pprof/profile", pprof.Profile)
	// app.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	// app.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ui.App = app

	proxy := http.NewServeMux()
	proxy.HandleFunc("/", Proxy(ui))
	ui.proxy = proxy

	return ui
}

func healthHandler(ui *WebUI) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s := ui.Server
		payload := map[string]interface{}{
			"now": util.Nows(),
			"server": map[string]interface{}{
				"description":     client.Name,
				"faktory_version": client.Version,
				"connections":     atomic.LoadUint64(&s.Stats.Connections),
				"command_count":   atomic.LoadUint64(&s.Stats.Commands),
			},
		}
		data, err := json.Marshal(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-Type", "application/json")
		w.Header().Add("Cache-Control", "no-cache")
		_, _ = w.Write(data)
	}
}

func Proxy(ui *WebUI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		///////
		// Support transparent proxying with nginx's proxy_pass.
		// Note that it's super critical that location == X-Script-Name
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
			r.URL.Path = strings.Replace(r.URL.Path, prefix, "", 1)
		}
		ui.App.ServeHTTP(w, r)
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
		WriteTimeout:   30 * time.Second,
		MaxHeaderBytes: 1 << 16,
		Handler:        ui.proxy,
	}

	go func() {
		var err error
		if ui.Server.TLSPrivateKey != "" {
			util.Infof("Web server now listening via TLS at %s", ui.Options.Binding)
			err = s.ListenAndServeTLS(ui.Server.TLSPublicCert, ui.Server.TLSPrivateKey)
		} else {
			util.Infof("Web server now listening at %s", ui.Options.Binding)
			err = s.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			util.Error(fmt.Sprintf("%s server crashed", ui.Options.Binding), err)
		}
	}()
	return func() { _ = s.Shutdown(context.Background()) }, nil
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

		dctx := NewContext(ui, r, w)

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
