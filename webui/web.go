package webui

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/util"
)

type Tab struct {
	Name string
	Path string
}

var (
	DefaultTabs = []Tab{
		Tab{"Home", "/"},
		Tab{"Busy", "/busy"},
		Tab{"Queues", "/queues"},
		Tab{"Retries", "/retries"},
		Tab{"Scheduled", "/scheduled"},
		Tab{"Dead", "/morgue"},
	}

	defaultServer *server.Server
	/*
	 * Since Go doesn't have thread-local storage,
	 * there's no way to get the t() function to
	 * look up request-specific translations without
	 * passing the request into the method, which is painful.
	 * Instead store a global variable for the current locale.
	 * This can cause pages to render with two different
	 * languages if two people are using the UI at the exact
	 * same time.
	 *
	 * TOOD Integrate context.Context.
	 */
	activeTranslations map[string]string
)

//go:generate ego .
//go:generate go-bindata -pkg webui -o static.go static/...

func init() {
	http.Handle("/static/", Cache(http.FileServer(&AssetFS{Asset: Asset, AssetDir: AssetDir})))
	http.HandleFunc("/stats", DebugLog(statsHandler))

	http.HandleFunc("/", Log(GetOnly(indexHandler)))
	http.HandleFunc("/queues", Log(queuesHandler))
	http.HandleFunc("/queues/", Log(queueHandler))
	http.HandleFunc("/retries", Log(retriesHandler))
	http.HandleFunc("/retries/", Log(retryHandler))
	http.HandleFunc("/scheduled", Log(scheduledHandler))
	http.HandleFunc("/scheduled/", Log(scheduledJobHandler))
	http.HandleFunc("/morgue", Log(morgueHandler))
	http.HandleFunc("/morgue/", Log(deadHandler))
	http.HandleFunc("/busy", Log(busyHandler))
	http.HandleFunc("/debug", Log(debugHandler))
	initLocales()

	server.OnStart(FireItUp)
}

var (
	Password    = ""
	locales     = map[string]map[string]string{}
	localeMutex = sync.Mutex{}
)

func initLocales() {
	localeFiles, err := AssetDir("static/locales")
	if err != nil {
		panic(err)
	}
	for _, filename := range localeFiles {
		name := strings.Split(filename, ".")[0]
		locales[name] = nil
	}
	translations("en") // eager load English
	util.Debugf("Initialized %d locales", len(localeFiles))
}

func translations(locale string) map[string]string {
	localeMutex.Lock()
	strs, ok := locales[locale]
	localeMutex.Unlock()
	if strs != nil {
		return strs
	}

	if !ok {
		return nil
	}

	if ok {
		util.Debugf("Booting the %s locale", locale)
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
		localeMutex.Lock()
		locales[locale] = strs
		localeMutex.Unlock()
		return strs
	}

	panic("Shouldn't get here")
}

func FireItUp(svr *server.Server) error {
	defaultServer = svr
	go func() {
		s := &http.Server{
			Addr:           ":7420",
			ReadTimeout:    1 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
			TLSConfig:      svr.TLSConfig,
		}
		if s.TLSConfig == nil {
			util.Info("Web server now listening on port 7420")
			log.Fatal(s.ListenAndServe())
		} else {
			util.Info("Web server now listening securely on port 7420")
			log.Fatal(s.ListenAndServeTLS("", ""))
		}
	}()
	return nil
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
func DebugLog(pass http.HandlerFunc) http.HandlerFunc {
	return Setup(pass, true)
}

func Log(pass http.HandlerFunc) http.HandlerFunc {
	return Setup(pass, false)
}

func Setup(pass http.HandlerFunc, debug bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// this is the entry point for every dynamic request
		// static assets bypass all this hubbub
		start := time.Now()

		// negotiate the language to be used for rendering
		locale := localeFromHeader(r.Header.Get("Accept-Language"))
		w.Header().Set("Content-Language", locale)

		dctx := &DefaultContext{
			Context:  r.Context(),
			response: w,
			request:  r,
			locale:   locale,
			strings:  translations(locale),
		}

		pass(w, r.WithContext(dctx))
		if debug {
			util.Debugf("%s %s %v", r.Method, r.RequestURI, time.Now().Sub(start))
		} else {
			util.Infof("%s %s %v", r.Method, r.RequestURI, time.Now().Sub(start))
		}
	}
}

func BasicAuth(pass http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="faktory"`)
			return
		}
		if subtle.ConstantTimeCompare([]byte(password), []byte(Password)) == 1 {
			http.Error(w, "authorization failed", http.StatusUnauthorized)
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

func Cache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Cache-Control", "public, max-age=3600")
		h.ServeHTTP(w, r)
	})
}
