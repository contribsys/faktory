package webui

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/server"
)

func statsHandler(w http.ResponseWriter, r *http.Request) {
	hash, err := server.CurrentState(defaultServer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("Cache-Control", "no-cache")
	w.Write(data)
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

	if r.Method == "POST" {
		r.ParseForm()
		keys := r.Form["bkey"]
		if len(keys) > 0 {
			bkeys := make([][]byte, len(keys))
			for idx, key := range keys {
				bindata, err := base64.RawURLEncoding.DecodeString(key)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bkeys[idx] = bindata
			}
			err := q.Delete(bkeys)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		http.Redirect(w, r, "/queues/"+queueName, http.StatusFound)
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

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Redirect(w, r, "/retries", http.StatusFound)
		}
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
	data, err := defaultServer.Store().Retries().Get([]byte(key))
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
		http.Error(w, fmt.Sprintf("Job %s is not a retry", job.Jid), http.StatusInternalServerError)
		return
	}
	ego_retry(w, r, key, &job)
}

func scheduledHandler(w http.ResponseWriter, r *http.Request) {
	set := defaultServer.Store().Scheduled()

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Redirect(w, r, "/scheduled", http.StatusFound)
		}
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

	ego_listScheduled(w, r, set, count, currentPage)
}

func scheduledJobHandler(w http.ResponseWriter, r *http.Request) {
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

	data, err := defaultServer.Store().Scheduled().Get([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the /retries page
		http.Redirect(w, r, "/scheduled", http.StatusTemporaryRedirect)
		return
	}

	var job faktory.Job
	err = json.Unmarshal(data, &job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if job.At == "" {
		http.Error(w, fmt.Sprintf("Job %s is not scheduled", job.Jid), http.StatusInternalServerError)
		return
	}
	ego_scheduled_job(w, r, key, &job)
}

func morgueHandler(w http.ResponseWriter, r *http.Request) {
	set := defaultServer.Store().Dead()

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Redirect(w, r, "/morgue", http.StatusFound)
		}
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

	ego_listDead(w, r, set, count, currentPage)
}

func deadHandler(w http.ResponseWriter, r *http.Request) {
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
	data, err := defaultServer.Store().Dead().Get([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the listing page
		http.Redirect(w, r, "/morgue", http.StatusTemporaryRedirect)
		return
	}

	var job faktory.Job
	err = json.Unmarshal(data, &job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ego_dead(w, r, key, &job)
}

func busyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		wid := r.FormValue("wid")
		action := r.FormValue("signal")
		if wid != "" {
			for _, client := range defaultServer.Heartbeats() {
				if wid == "all" {
					client.Signal(action)
				} else if wid == client.Wid {
					client.Signal(action)
					break
				}
			}
		}
		http.Redirect(w, r, "/busy", http.StatusFound)
		return
	}
	ego_busy(w, r)
}

func debugHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var err error

		action := r.FormValue("action")
		switch action {
		case "backup":
			err = defaultServer.Store().Backup()
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Redirect(w, r, "/debug", http.StatusFound)
		}
		return
	}

	ego_debug(w, r)
}
