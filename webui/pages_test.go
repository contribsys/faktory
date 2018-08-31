package webui

import (
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

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestPages(t *testing.T) {
	bootRuntime(t, "pages", func(ui *WebUI, s *server.Server, t *testing.T) {

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
		})

		t.Run("Queues", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/queues", nil)
			assert.NoError(t, err)

			str := s.Store()
			str.GetQueue("default")
			q, _ := str.GetQueue("foobar")
			q.Clear()
			q.Push(5, []byte("1l23j12l3"))

			w := httptest.NewRecorder()
			queuesHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "default"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())
		})

		t.Run("Queue", func(t *testing.T) {
			s.Store().Flush()
			req, err := ui.NewRequest("GET", "http://localhost:7420/queues/foobar", nil)
			assert.NoError(t, err)

			str := s.Store()
			q, _ := str.GetQueue("foobar")
			q.Clear()
			q.Push(5, []byte(`{"jobtype":"SomeWorker","args":["1l23j12l3"],"queue":"foobar"}`))
			assert.EqualValues(t, 1, q.Size())

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

			assert.EqualValues(t, 0, q.Size())
			assert.Equal(t, 302, w.Code)
		})

		t.Run("Retries", func(t *testing.T) {
			s.Store().Flush()
			req, err := ui.NewRequest("GET", "http://localhost:7420/retries", nil)
			assert.NoError(t, err)

			str := s.Store()
			retries := str.Retries()
			retries.Clear()
			jid1, data := fakeJob()
			err = retries.AddElement(util.Nows(), jid1, data)
			assert.NoError(t, err)

			jid2, data := fakeJob()
			err = retries.AddElement(util.Nows(), jid2, data)
			assert.NoError(t, err)

			var key []byte
			retries.Each(func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			keys := string(key)

			w := httptest.NewRecorder()
			retriesHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid1), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), jid2), w.Body.String())

			def, err := str.GetQueue("default")
			assert.NoError(t, err)
			sz := def.Size()
			cnt, err := def.Clear()
			assert.NoError(t, err)
			assert.Equal(t, sz, cnt)

			assert.EqualValues(t, 0, def.Size())
			assert.EqualValues(t, 2, retries.Size())
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
			assert.EqualValues(t, 1, retries.Size())
			assert.EqualValues(t, 1, def.Size())

			err = retries.Each(func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			assert.NoError(t, err)

			keys = string(key)
			payload = url.Values{
				"key":    {keys},
				"action": {"kill"},
			}
			assert.EqualValues(t, 0, str.Dead().Size())
			req, err = ui.NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			retriesHandler(w, req)

			assert.Equal(t, "", w.Body.String())
			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 0, retries.Size())
			assert.EqualValues(t, 1, str.Dead().Size())
		})

		t.Run("Retry", func(t *testing.T) {
			str := s.Store()
			q := str.Retries()
			q.Clear()
			jid, data := fakeJob()
			ts := util.Nows()

			err := q.AddElement(ts, jid, data)
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
			q.Clear()
			jid, data := fakeJob()

			err = q.AddElement(util.Nows(), jid, data)
			assert.NoError(t, err)

			var key []byte
			q.Each(func(idx int, entry storage.SortedEntry) error {
				key, err = entry.Key()
				return err
			})
			keys := string(key)

			w := httptest.NewRecorder()
			scheduledHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "SomeWorker"), w.Body.String())
			assert.True(t, strings.Contains(w.Body.String(), keys), w.Body.String())

			assert.EqualValues(t, 1, q.Size())
			payload := url.Values{
				"key":    {keys},
				"action": {"delete"},
			}
			req, err = ui.NewRequest("POST", "http://localhost:7420/scheduled", strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			scheduledHandler(w, req)

			assert.Equal(t, 302, w.Code)
			assert.EqualValues(t, 0, q.Size())
			assert.False(t, strings.Contains(w.Body.String(), keys), w.Body.String())
		})

		t.Run("ScheduledJob", func(t *testing.T) {
			str := s.Store()
			q := str.Scheduled()
			q.Clear()
			jid, data := fakeJob()
			ts := util.Thens(time.Now().Add(1e6 * time.Second))

			err := q.AddElement(ts, jid, data)
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
			q.Clear()
			jid, data := fakeJob()

			err = q.AddElement(util.Nows(), jid, data)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			morgueHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
		})

		t.Run("Dead", func(t *testing.T) {
			str := s.Store()
			q := str.Dead()
			q.Clear()
			jid, data := fakeJob()
			ts := util.Nows()

			err := q.AddElement(ts, jid, data)
			assert.NoError(t, err)

			req, err := ui.NewRequest("GET", fmt.Sprintf("http://localhost:7420/morgue/%s|%s", ts, jid), nil)
			assert.NoError(t, err)
			w := httptest.NewRecorder()
			deadHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())

			assert.EqualValues(t, 1, q.Size())
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
			assert.EqualValues(t, 0, q.Size())
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
			var hndlr http.HandlerFunc = Setup(ui, busyHandler, false)
			csrfBusyHandler := Protect(true, hndlr)
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
			var hndlr http.HandlerFunc = Setup(ui, busyHandler, false)
			csrfBusyHandler := Protect(false, hndlr)
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

func (ui *WebUI) NewRequest(method string, url string, body io.Reader) (*http.Request, error) {
	r := httptest.NewRequest(method, url, body)
	dctx := &DefaultContext{
		Context: r.Context(),
		webui:   ui,
		request: r,
		locale:  "en",
		strings: translations("en"),
	}
	return r.WithContext(dctx), nil
}

func findCSRFTokens(w http.ResponseWriter, body string) (string, string) {
	bodyToken := ""
	cookieToken := ""

	// parse body token
	searchBody, _ := regexp.Compile(`name="csrf_token" value="(.*)"/>`)
	searchCookie, _ := regexp.Compile(`csrf_token=(.*);`)
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
