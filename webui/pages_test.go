package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestPages(t *testing.T) {
	bootRuntime(t, "pages", func(ui *WebUI, s *server.Server, t *testing.T) {
		bg := context.Background()

		t.Run("Index", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			indexHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "uptime_in_days"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "idle"), w.Body.String())
		})

		t.Run("Stats", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/stats", nil)
			assert.NoError(t, err)

			s.Stats.StartedAt = time.Now().Add(-1234567 * time.Second)
			str := s.Store()
			_, err = str.GetQueue(bg, "default")
			assert.NoError(t, err)
			q, err := str.GetQueue(bg, "foobar")
			assert.NoError(t, err)
			_, err = q.Clear(bg)
			assert.NoError(t, err)
			args := []string{"faktory", "rocks", "!!", ":)"}
			for _, v := range args {
				err = q.Push(bg, []byte(v))
				assert.NoError(t, err)
			}

			w := httptest.NewRecorder()
			statsHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

			var content map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &content)
			assert.NoError(t, err)

			s := content["server"].(map[string]interface{})
			uid := s["uptime"].(float64)
			assert.Equal(t, float64(1234567), uid)

			queues := content["faktory"].(map[string]interface{})["queues"].(map[string]interface{})
			defaultQ := queues["default"].(float64)
			assert.Equal(t, 0.0, defaultQ)
			foobarQ := queues["foobar"]
			assert.Nil(t, foobarQ)
		})

		t.Run("Queues", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/queues", nil)
			assert.NoError(t, err)

			str := s.Store()
			_, err = str.GetQueue(bg, "default")
			assert.NoError(t, err)
			q, err := str.GetQueue(bg, "foobar")
			assert.NoError(t, err)
			_, err = q.Clear(bg)
			assert.NoError(t, err)
			err = q.Push(bg, []byte("1l23j12l3"))
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			queuesHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "default"), w.Body.String())
			assert.False(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())
		})

		t.Run("Queue", func(t *testing.T) {
			s.Store().Flush(bg)
			req, err := ui.NewRequest("GET", "http://localhost:7420/queues/foobar", nil)
			assert.NoError(t, err)

			str := s.Store()
			q, err := str.GetQueue(bg, "foobar")
			assert.NoError(t, err)
			_, err = q.Clear(bg)
			assert.NoError(t, err)

			job := client.NewJob("SomeWorker", "1l23j12l3")
			job.Queue = "foobar"

			err = s.Manager().Push(bg, job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))

			w := httptest.NewRecorder()
			queueHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "1l23j12l3"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())

			payload := url.Values{
				"action": {"delete"},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/queue/"+q.Name(), strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			queueHandler(w, req)

			assert.EqualValues(t, 0, q.Size(bg))
			assert.Equal(t, 302, w.Code)
		})

		t.Run("Retries", func(t *testing.T) {
			s.Store().Flush(bg)
			req, err := ui.NewRequest("GET", "http://localhost:7420/retries", nil)
			assert.NoError(t, err)

			str := s.Store()
			retries := str.Retries()
			err = retries.Clear(bg)
			assert.NoError(t, err)
			jid1, data := fakeJob()
			err = retries.AddElement(bg, util.Nows(), jid1, data)
			assert.NoError(t, err)

			jid2, data := fakeJob()
			err = retries.AddElement(bg, util.Nows(), jid2, data)
			assert.NoError(t, err)

			jid3, data := fakeJob()
			err = retries.AddElement(bg, util.Nows(), jid3, data)
			assert.NoError(t, err)

			var key []byte
			err = retries.Each(bg, func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			assert.NoError(t, err)
			keys := string(key)

			w := httptest.NewRecorder()
			retriesHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid1), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), jid2), w.Body.String())

			def, err := str.GetQueue(bg, "default")
			assert.NoError(t, err)
			sz := def.Size(bg)
			cnt, err := def.Clear(bg)
			assert.NoError(t, err)
			assert.Equal(t, sz, cnt)

			assert.EqualValues(t, 0, def.Size(bg))
			assert.EqualValues(t, 3, retries.Size(bg))
			payload := url.Values{
				"key":    {keys},
				"action": {"retry"},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			retriesHandler(w, req)

			assert.Equal(t, "", w.Body.String())
			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 2, retries.Size(bg))
			assert.EqualValues(t, 1, def.Size(bg))

			err = retries.Each(bg, func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			assert.NoError(t, err)

			keys = string(key)
			payload = url.Values{
				"key":    {keys},
				"action": {"kill"},
			}
			assert.EqualValues(t, 0, str.Dead().Size(bg))
			req, err = ui.NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			retriesHandler(w, req)

			assert.Equal(t, "", w.Body.String())
			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 1, retries.Size(bg))
			assert.EqualValues(t, 1, str.Dead().Size(bg))

			// Now try to operate on an element which disappears by clearing the sset
			// manually before submitting
			err = retries.Each(bg, func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			assert.NoError(t, err)

			keys = string(key)
			payload = url.Values{
				"key":    {keys},
				"action": {"kill"},
			}
			err = retries.Clear(bg) // clear it under us!
			assert.NoError(t, err)
			req, err = ui.NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			retriesHandler(w, req)

			assert.Equal(t, "", w.Body.String())
			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 0, retries.Size(bg))
			assert.EqualValues(t, 1, str.Dead().Size(bg))
		})

		t.Run("Retry", func(t *testing.T) {
			str := s.Store()
			q := str.Retries()
			err := q.Clear(bg)
			assert.NoError(t, err)
			jid, data := fakeJob()
			ts := util.Nows()

			err = q.AddElement(bg, ts, jid, data)
			assert.NoError(t, err)

			req, err := ui.NewRequest("GET", fmt.Sprintf("http://localhost:7420/retries/%s|%s", ts, jid), nil)
			assert.NoError(t, err)
			w := httptest.NewRecorder()
			retryHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
		})

		t.Run("Scheduled", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/scheduled", nil)
			assert.NoError(t, err)

			str := s.Store()
			q := str.Scheduled()
			err = q.Clear(bg)
			assert.NoError(t, err)
			jid, data := fakeJob()

			err = q.AddElement(bg, util.Nows(), jid, data)
			assert.NoError(t, err)

			var key []byte
			err = q.Each(bg, func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			assert.NoError(t, err)
			keys := string(key)

			w := httptest.NewRecorder()
			scheduledHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "SomeWorker"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), keys), w.Body.String())

			assert.EqualValues(t, 1, q.Size(bg))
			payload := url.Values{
				"key":    {keys},
				"action": {"add_to_queue"},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/scheduled", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			scheduledHandler(w, req)

			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.False(t, strings.Contains(w.Body.String(), keys), w.Body.String())
		})

		t.Run("ScheduledJob", func(t *testing.T) {
			str := s.Store()
			q := str.Scheduled()
			err := q.Clear(bg)
			assert.NoError(t, err)
			jid, data := fakeJob()
			ts := util.Thens(time.Now().Add(1e6 * time.Second))

			err = q.AddElement(bg, ts, jid, data)
			assert.NoError(t, err)

			req, err := ui.NewRequest("GET", fmt.Sprintf("http://localhost:7420/scheduled/%s|%s", ts, jid), nil)
			assert.NoError(t, err)
			w := httptest.NewRecorder()
			scheduledJobHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
		})

		t.Run("Morgue", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/morgue", nil)
			assert.NoError(t, err)

			str := s.Store()
			q := str.Dead()
			err = q.Clear(bg)
			assert.NoError(t, err)
			jid, data := fakeJob()

			err = q.AddElement(bg, util.Nows(), jid, data)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			morgueHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
		})

		t.Run("Dead", func(t *testing.T) {
			str := s.Store()
			q := str.Dead()
			err := q.Clear(bg)
			assert.NoError(t, err)
			jid, data := fakeJob()
			ts := util.Nows()

			err = q.AddElement(bg, ts, jid, data)
			assert.NoError(t, err)

			req, err := ui.NewRequest("GET", fmt.Sprintf("http://localhost:7420/morgue/%s|%s", ts, jid), nil)
			assert.NoError(t, err)
			w := httptest.NewRecorder()
			deadHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())

			assert.EqualValues(t, 1, q.Size(bg))
			payload := url.Values{
				"key":    {"all"},
				"action": {"delete"},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/morgue", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			morgueHandler(w, req)

			assert.Equal(t, 302, w.Code)
			assert.Equal(t, "", w.Body.String())
			assert.EqualValues(t, 0, q.Size(bg))
		})

		t.Run("Busy", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/busy", nil)
			assert.NoError(t, err)

			wid := "1239123oim,bnsad"
			wrk := &server.ClientData{
				Hostname:  "foobar.local",
				Pid:       12345,
				Wid:       wid,
				Labels:    []string{"bubba"},
				StartedAt: time.Now(),
				Version:   2,
			}
			s.Heartbeats()[wid] = wrk

			w := httptest.NewRecorder()
			busyHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), wid), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "foobar.local"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "bubba"), w.Body.String())
			assert.False(t, wrk.IsQuiet())

			data := url.Values{
				"signal": {"quiet"},
				"wid":    {wid},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			busyHandler(w, req)
			assert.Equal(t, 302, w.Code)
			assert.True(t, wrk.IsQuiet())
		})

		t.Run("RequireCSRF", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/busy", nil)
			assert.NoError(t, err)

			wid := "1239123oim,bnsad"
			wrk := &server.ClientData{
				Hostname:  "foobar.local",
				Pid:       12345,
				Wid:       wid,
				Labels:    []string{"bubba"},
				StartedAt: time.Now(),
				Version:   2,
			}
			s.Heartbeats()[wid] = wrk

			w := httptest.NewRecorder()

			// Wrap the handler with CSRF protection
			var hndlr http.HandlerFunc = setup(ui, busyHandler, false)
			csrfBusyHandler := protect(true, hndlr)
			csrfBusyHandler.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), wid), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "foobar.local"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "bubba"), w.Body.String())
			assert.False(t, wrk.IsQuiet())

			// Retrieve the CSRF token needed in the future POST request
			token, cookieToken := findCSRFTokens(w, w.Body.String())

			// Make the POST request without any CSRF tokens present
			data := url.Values{
				"signal": {"quiet"},
				"wid":    {wid},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))

			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			csrfBusyHandler.ServeHTTP(w, req)

			// Request without CSRF should fail
			assert.Equal(t, 400, w.Code)

			// Make the POST request including the CSRF token
			data = url.Values{
				"signal":     {"quiet"},
				"wid":        {wid},
				"csrf_token": {token},
			}

			req, err = ui.NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))

			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Cookie", "csrf_token="+cookieToken)
			w = httptest.NewRecorder()
			csrfBusyHandler.ServeHTTP(w, req)

			// Request with CSRF should pass
			assert.Equal(t, 302, w.Code)
			assert.True(t, wrk.IsQuiet())
		})

		t.Run("RespectUseCSRFGlobal", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/busy", nil)
			assert.NoError(t, err)

			wid := "1239123oim,bnsad"
			wrk := &server.ClientData{
				Hostname:  "foobar.local",
				Pid:       12345,
				Wid:       wid,
				Labels:    []string{"bubba"},
				StartedAt: time.Now(),
				Version:   2,
			}
			s.Heartbeats()[wid] = wrk

			w := httptest.NewRecorder()

			// Wrap the handler with CSRF protection
			var hndlr http.HandlerFunc = setup(ui, busyHandler, false)
			csrfBusyHandler := protect(false, hndlr)
			csrfBusyHandler.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			assert.True(t, strings.Contains(w.Body.String(), wid), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "foobar.local"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "bubba"), w.Body.String())
			assert.False(t, wrk.IsQuiet())

			// Make the POST request without any CSRF tokens present
			data := url.Values{
				"signal": {"quiet"},
				"wid":    {wid},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))

			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			csrfBusyHandler.ServeHTTP(w, req)

			// Request without CSRF should pass
			assert.Equal(t, 302, w.Code)
			assert.True(t, wrk.IsQuiet())
		})
	})
}

func (ui *WebUI) NewRequest(method string, urlstr string, body io.Reader) (*http.Request, error) {
	r := httptest.NewRequest(method, urlstr, body)
	dctx := &DefaultContext{
		Context: r.Context(),
		webui:   ui,
		request: r,
		locale:  "en",
		strings: translations("en"),
	}
	return r.WithContext(dctx), nil
}

var (
	searchBody   = regexp.MustCompile(`name="csrf_token" value="(.*)"/>`)
	searchCookie = regexp.MustCompile(`csrf_token=(.*);`)
)

func findCSRFTokens(w http.ResponseWriter, body string) (string, string) {
	bodyToken := ""
	cookieToken := ""

	// parse body token
	results := searchBody.FindStringSubmatch(body)
	if len(results) > 1 {
		fmt.Println(results)
		bodyToken = results[1]
	}

	// parse header token
	rawCookie := w.Header().Get("Set-Cookie")
	if rawCookie != "" {
		results2 := searchCookie.FindStringSubmatch(rawCookie)
		if len(results2) > 1 {
			cookieToken = results2[1]
			fmt.Println(rawCookie)
		}
	}
	return bodyToken, cookieToken
}
