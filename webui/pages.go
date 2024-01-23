package webui

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/contribsys/faktory/server"
)

func statsHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	hash, err := c.Server().CurrentState()
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
	_, _ = w.Write(data)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	if c.Server() == nil {
		http.Error(w, "Server not booted", http.StatusInternalServerError)
		return
	}

	Render("index", w, &PageData{r, 0, 0, 0, nil, ""})
}

func queuesHandler(w http.ResponseWriter, r *http.Request) {
	Render("queues", w, &PageData{r, 0, 0, 0, nil, ""})
}

var (
	LAST_ELEMENT = regexp.MustCompile(`/([^/]+)\z`)
)

func queueHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	name := LAST_ELEMENT.FindStringSubmatch(r.URL.Path)
	if name == nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}
	queueName := name[1]
	q, ok := c.Store().ExistingQueue(c, queueName)
	if !ok {
		Redirect(w, r, "/queues", http.StatusFound)
		return
	}

	if r.Method == "POST" {
		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		keys := r.Form["bkey"]
		if len(keys) > 0 {
			// delete specific entries
			bkeys := make([][]byte, len(keys))
			for idx := range keys {
				bindata, err := base64.RawURLEncoding.DecodeString(keys[idx])
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bkeys[idx] = bindata
			}
			err := q.Delete(c, bkeys)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			action := r.FormValue("action")
			if action == "delete" {
				// clear entire queue
				_, err := q.Clear(c)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			} else if action == "pause" {
				err := c.webui.Server.Manager().PauseQueue(c, q.Name())
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			} else if action == "resume" {
				err := c.webui.Server.Manager().ResumeQueue(c, q.Name())
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}

			Redirect(w, r, "/queues", http.StatusFound)
			return
		}
		Redirect(w, r, "/queues/"+queueName, http.StatusFound)
		return
	}

	currentPage := uint64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = uint64(val)
	}
	count := uint64(25)
	total := q.Size(c)

	Render("queue", w, &PageData{r, currentPage, count, total, q, ""})
}

func retriesHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	set := c.Store().Retries()

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(r, set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			Redirect(w, r, "/retries", http.StatusFound)
		}
		return
	}

	currentPage := uint64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = uint64(val)
	}
	count := uint64(25)
	total := set.Size(c)

	Render("retries", w, &PageData{r, currentPage, count, total, set, ""})
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

	c := r.Context().(*DefaultContext)
	set := c.Store().Retries()
	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := []string{key}
		err := actOn(r, set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			Redirect(w, r, "/retries", http.StatusFound)
		}
		return
	}

	data, err := set.Get(c, []byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the /retries page
		Redirect(w, r, "/retries", http.StatusTemporaryRedirect)
		return
	}

	job, err := data.Job()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if job.Failure == nil {
		http.Error(w, fmt.Sprintf("Job %s is not a retry", job.Jid), http.StatusInternalServerError)
		return
	}
	Render("retry", w, &PageData{r, 0, 0, 0, job, key})
}

func scheduledHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	set := c.Store().Scheduled()

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(r, set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			Redirect(w, r, "/scheduled", http.StatusFound)
		}
		return
	}

	currentPage := uint64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = uint64(val)
	}
	count := uint64(25)
	total := set.Size(c)

	Render("scheduled", w, &PageData{r, currentPage, count, total, set, ""})
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

	c := r.Context().(*DefaultContext)
	set := c.Store().Scheduled()
	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := []string{key}
		err := actOn(r, set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			Redirect(w, r, "/scheduled", http.StatusFound)
		}
		return
	}

	data, err := set.Get(c, []byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the /retries page
		Redirect(w, r, "/scheduled", http.StatusTemporaryRedirect)
		return
	}

	job, err := data.Job()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	Render("scheduled_job", w, &PageData{r, 0, 0, 0, job, key})
}

func morgueHandler(w http.ResponseWriter, r *http.Request) {
	c := r.Context().(*DefaultContext)
	set := c.Store().Dead()

	if r.Method == "POST" {
		action := r.FormValue("action")
		keys := r.Form["key"]
		err := actOn(r, set, action, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			Redirect(w, r, "/morgue", http.StatusFound)
		}
		return
	}

	currentPage := uint64(1)
	p := r.URL.Query()["page"]
	if p != nil {
		val, err := strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, "Invalid parameter", http.StatusBadRequest)
			return
		}
		currentPage = uint64(val)
	}
	count := uint64(25)
	total := set.Size(c)

	Render("morgue", w, &PageData{r, currentPage, count, total, set, ""})
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
	c := r.Context().(*DefaultContext)
	data, err := c.Store().Dead().Get(c, []byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if data == nil {
		// retry has disappeared?  possibly requeued while the user was sitting on the listing page
		Redirect(w, r, "/morgue", http.StatusTemporaryRedirect)
		return
	}

	job, err := data.Job()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	Render("dead", w, &PageData{r, 0, 0, 0, job, key})
}

func busyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		wid := r.FormValue("wid")
		action := r.FormValue("signal")
		if wid != "" {
			var signal server.WorkerState
			if action == "quiet" {
				signal = server.Quiet
			} else if action == "terminate" {
				signal = server.Terminate
			} else {
				http.Error(w, fmt.Sprintf("Invalid signal: %s", action), http.StatusInternalServerError)
				return
			}

			c := r.Context().(*DefaultContext)
			for _, client := range c.Server().Heartbeats() {
				if wid == "all" || wid == client.Wid {
					client.Signal(signal)
				}
			}
		}
		Redirect(w, r, "/busy", http.StatusFound)
		return
	}
	Render("busy", w, &PageData{r, 0, 0, 0, nil, ""})
}

func debugHandler(w http.ResponseWriter, r *http.Request) {
	Render("debug", w, &PageData{r, 0, 0, 0, nil, ""})
}

func Redirect(w http.ResponseWriter, r *http.Request, path string, code int) {
	c := r.Context().(*DefaultContext)
	http.Redirect(w, r, fmt.Sprintf("%s%s", c.Root, path), code)
}
