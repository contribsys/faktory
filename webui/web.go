package webui

import (
	"log"
	"net/http"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/util"
)

var (
	DefaultTabs = map[string]string{
		"Dashboard": "",
		"Busy":      "busy",
		"Queues":    "queues",
		"Retries":   "retries",
		"Scheduled": "scheduled",
		"Dead":      "morgue",
	}
)

//go:generate ego .
//go:generate go-bindata -pkg webui -o static.go static/...

func init() {
	http.Handle("/static/", http.FileServer(
		&AssetFS{Asset: Asset, AssetDir: AssetDir}))
	http.HandleFunc("/", indexHandler)
	faktory.OnStart(FireItUp)
}

var (
	Password = ""
)

func FireItUp(svr *faktory.Server) {
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
	index(w)
}
