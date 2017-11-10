package webui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func init() {
	bootRuntime()
}

func TestIndex(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/", nil)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	indexHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "uptime_in_days"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "idle"), w.Body.String())
}

func TestStats(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/stats", nil)
	assert.NoError(t, err)

	defaultServer.Stats.StartedAt = time.Now().Add(-1234567 * time.Second)

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
}

func TestQueues(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/queues", nil)
	assert.NoError(t, err)

	str := defaultServer.Store()
	str.GetQueue("default")
	q, _ := str.GetQueue("foobar")
	q.Clear()
	q.Push(5, []byte("1l23j12l3"))

	w := httptest.NewRecorder()
	queuesHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "default"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())
}

func TestQueue(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/queues/foobar", nil)
	assert.NoError(t, err)

	str := defaultServer.Store()
	q, _ := str.GetQueue("foobar")
	q.Clear()
	q.Push(5, []byte(`{"jobtype":"SomeWorker","args":["1l23j12l3"],"queue":"foobar"}`))

	w := httptest.NewRecorder()
	queueHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "1l23j12l3"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())

	assert.Equal(t, uint64(1), q.Size())
	payload := url.Values{
		"action": {"delete"},
	}
	req, err = NewRequest("POST", "http://localhost:7420/queue/"+q.Name(), strings.NewReader(payload.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	queueHandler(w, req)

	assert.Equal(t, uint64(0), q.Size())
	assert.Equal(t, 302, w.Code)
}

func TestRetries(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/retries", nil)
	assert.NoError(t, err)

	str := defaultServer.Store()
	q := str.Retries()
	q.Clear()
	jid1, data := fakeJob()
	err = q.AddElement(util.Nows(), jid1, data)
	assert.NoError(t, err)

	jid2, data := fakeJob()
	err = q.AddElement(util.Nows(), jid2, data)
	assert.NoError(t, err)

	var key []byte
	q.Each(func(idx int, k, v []byte) error {
		key = make([]byte, len(k))
		copy(key, k)
		return nil
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

	assert.Equal(t, uint64(0), def.Size())
	assert.Equal(t, int64(2), q.Size())
	payload := url.Values{
		"key":    {keys, "abadone"},
		"action": {"retry"},
	}
	req, err = NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	retriesHandler(w, req)

	assert.Equal(t, 302, w.Code)
	assert.Equal(t, int64(1), q.Size())
	assert.Equal(t, uint64(1), def.Size())

	q.Each(func(idx int, k, v []byte) error {
		key = make([]byte, len(k))
		copy(key, k)
		return nil
	})
	keys = string(key)
	payload = url.Values{
		"key":    {keys},
		"action": {"kill"},
	}
	assert.Equal(t, int64(0), str.Dead().Size())
	req, err = NewRequest("POST", "http://localhost:7420/retries", strings.NewReader(payload.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	retriesHandler(w, req)

	assert.Equal(t, 302, w.Code)
	assert.Equal(t, int64(0), q.Size())
	assert.Equal(t, int64(1), str.Dead().Size())
}

func TestRetry(t *testing.T) {
	str := defaultServer.Store()
	q := str.Retries()
	q.Clear()
	jid, data := fakeJob()
	ts := util.Nows()

	err := q.AddElement(ts, jid, data)
	assert.NoError(t, err)

	req, err := NewRequest("GET", fmt.Sprintf("http://localhost:7420/retries/%s|%s", ts, jid), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	retryHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestScheduled(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/scheduled", nil)
	assert.NoError(t, err)

	str := defaultServer.Store()
	q := str.Scheduled()
	q.Clear()
	jid, data := fakeJob()

	err = q.AddElement(util.Nows(), jid, data)
	assert.NoError(t, err)

	var key []byte
	q.Each(func(idx int, k, v []byte) error {
		key = make([]byte, len(k))
		copy(key, k)
		return nil
	})
	keys := string(key)

	w := httptest.NewRecorder()
	scheduledHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "SomeWorker"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), keys), w.Body.String())

	assert.Equal(t, int64(1), q.Size())
	payload := url.Values{
		"key":    {keys},
		"action": {"delete"},
	}
	req, err = NewRequest("POST", "http://localhost:7420/scheduled", strings.NewReader(payload.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	scheduledHandler(w, req)

	assert.Equal(t, 302, w.Code)
	assert.Equal(t, int64(0), q.Size())
	assert.False(t, strings.Contains(w.Body.String(), keys), w.Body.String())
}

func TestScheduledJob(t *testing.T) {
	str := defaultServer.Store()
	q := str.Scheduled()
	q.Clear()
	jid, data := fakeJob()
	ts := util.Thens(time.Now().Add(1e6 * time.Second))

	err := q.AddElement(ts, jid, data)
	assert.NoError(t, err)

	req, err := NewRequest("GET", fmt.Sprintf("http://localhost:7420/scheduled/%s|%s", ts, jid), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	scheduledJobHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestMorgue(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/morgue", nil)
	assert.NoError(t, err)

	str := defaultServer.Store()
	q := str.Dead()
	q.Clear()
	jid, data := fakeJob()

	err = q.AddElement(util.Nows(), jid, data)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	morgueHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestDead(t *testing.T) {
	str := defaultServer.Store()
	q := str.Dead()
	q.Clear()
	jid, data := fakeJob()
	ts := util.Nows()

	err := q.AddElement(ts, jid, data)
	assert.NoError(t, err)

	req, err := NewRequest("GET", fmt.Sprintf("http://localhost:7420/morgue/%s|%s", ts, jid), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	deadHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())

	assert.Equal(t, int64(1), q.Size())
	payload := url.Values{
		"key":    {"all"},
		"action": {"delete"},
	}
	req, err = NewRequest("POST", "http://localhost:7420/morgue", strings.NewReader(payload.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	morgueHandler(w, req)

	assert.Equal(t, 302, w.Code)
	assert.Equal(t, "", w.Body.String())
	assert.Equal(t, int64(0), q.Size())
}

func TestBusy(t *testing.T) {
	req, err := NewRequest("GET", "http://localhost:7420/busy", nil)
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
	defaultServer.Heartbeats()[wid] = wrk

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
	req, err = NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	busyHandler(w, req)
	assert.Equal(t, 302, w.Code)
	assert.True(t, wrk.IsQuiet())
}

func NewRequest(method string, url string, body io.Reader) (*http.Request, error) {
	r := httptest.NewRequest(method, url, body)
	dctx := &DefaultContext{
		Context: r.Context(),
		request: r,
		locale:  "en",
		strings: translations("en"),
	}
	return r.WithContext(dctx), nil
}
