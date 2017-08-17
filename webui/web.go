package webui

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/mperham/faktory"
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
	http.HandleFunc("/queues/", Log(queueHandler))
	http.HandleFunc("/retries", Log(GetOnly(retriesHandler)))
	http.HandleFunc("/retries/", Log(retryHandler))
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

var (
	LAST_ELEMENT = regexp.MustCompile(`\/([^\/]+)\z`)
)

func queueHandler(w http.ResponseWriter, r *http.Request) {
	name := LAST_ELEMENT.FindStringSubmatch(r.RequestURI)
	if name == nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}
	queueName := name[1]
	q, err := defaultServer.Store().GetQueue(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	currentPage := int64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = int64(val)
	}
	count := int64(25)

	ego_queue(w, r, q, count, currentPage)
}

func retriesHandler(w http.ResponseWriter, r *http.Request) {
	set := defaultServer.Store().Retries()

	currentPage := int64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = int64(val)
	}
	count := int64(25)

	ego_listRetries(w, r, set, count, currentPage)
}

func retryHandler(w http.ResponseWriter, r *http.Request) {
	name := LAST_ELEMENT.FindStringSubmatch(r.RequestURI)
	if name == nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}
	key, err := url.QueryUnescape(name[1])
	if err != nil {
		http.Error(w, "Invalid URL input", http.StatusBadRequest)
		return
	}
	data, err := defaultServer.Store().Retries().GetElement(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the /retries page
		http.Redirect(w, r, "/retries", http.StatusTemporaryRedirect)
		return
	}

	var job faktory.Job
	err = json.Unmarshal(data, &job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if job.Failure == nil {
		panic("job is not a retry!" + string(data))
	}
	ego_retry(w, r, key, &job)
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
