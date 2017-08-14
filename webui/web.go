package webui

import (
	"crypto/subtle"
	"log"
	"net/http"
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
)

//go:generate ego .
//go:generate go-bindata -pkg webui -o static.go static/...

func init() {
	http.Handle("/favicon.ico", http.NotFoundHandler())
	http.Handle("/static/", http.FileServer(
		&AssetFS{Asset: Asset, AssetDir: AssetDir}))
	http.HandleFunc("/", Log(GetOnly(indexHandler)))
	http.HandleFunc("/queues", Log(GetOnly(queuesHandler)))
	server.OnStart(FireItUp)
}

var (
	Password = ""
)

func FireItUp(svr *server.Server) {
	defaultServer = svr
	go func() {
		s := &http.Server{
			Addr:           ":7420",
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		util.Info("Web server now listening on port 7420")
		log.Fatal(s.ListenAndServe())
	}()
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if defaultServer == nil {
		http.Error(w, "Server not booted", http.StatusInternalServerError)
		return
	}
	ego_index(w, r)
}

func queuesHandler(w http.ResponseWriter, r *http.Request) {
	ego_listQueues(w, r)
}

func Log(pass http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		pass(w, r)
		util.Infof("%s %s %v", r.Method, r.RequestURI, time.Now().Sub(start))
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
