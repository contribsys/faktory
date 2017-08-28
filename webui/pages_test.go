package webui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/", nil)
	assert.Nil(t, err)

	w := httptest.NewRecorder()
	indexHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "Hello World"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "idle"), w.Body.String())
}

func TestQueues(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/queues", nil)
	assert.Nil(t, err)

	str := defaultServer.Store()
	str.GetQueue("default")
	q, _ := str.GetQueue("foobar")
	q.Clear()
	q.Push([]byte("1l23j12l3"))

	w := httptest.NewRecorder()
	queuesHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "default"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())
}

func TestQueue(t *testing.T) {
	req := httptest.NewRequest("GET", "/queues/foobar", nil)

	str := defaultServer.Store()
	q, _ := str.GetQueue("foobar")
	q.Clear()
	q.Push([]byte(`{"jobtype":"SomeWorker","args":["1l23j12l3"],"queue":"foobar"}`))

	w := httptest.NewRecorder()
	queueHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "1l23j12l3"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "foobar"), w.Body.String())
}

func TestRetries(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/retries", nil)
	assert.Nil(t, err)

	str := defaultServer.Store()
	q := str.Retries()
	q.Clear()
	jid, data := fakeJob()

	err = q.AddElement(util.Nows(), jid, data)
	assert.Nil(t, err)

	w := httptest.NewRecorder()
	retriesHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestRetry(t *testing.T) {
	str := defaultServer.Store()
	q := str.Retries()
	q.Clear()
	jid, data := fakeJob()
	ts := util.Nows()

	err := q.AddElement(ts, jid, data)
	assert.Nil(t, err)

	req := httptest.NewRequest("GET", fmt.Sprintf("http://localhost:7420/retries/%s|%s", ts, jid), nil)
	w := httptest.NewRecorder()
	retryHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestScheduled(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/scheduled", nil)
	assert.Nil(t, err)

	str := defaultServer.Store()
	q := str.Scheduled()
	q.Clear()
	jid, data := fakeJob()

	err = q.AddElement(util.Nows(), jid, data)
	assert.Nil(t, err)

	w := httptest.NewRecorder()
	retriesHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), "SomeWorker"), w.Body.String())
}

func TestScheduledJob(t *testing.T) {
	str := defaultServer.Store()
	q := str.Scheduled()
	q.Clear()
	jid, data := fakeJob()
	ts := util.Thens(time.Now().Add(1e6 * time.Second))

	err := q.AddElement(ts, jid, data)
	assert.Nil(t, err)

	req := httptest.NewRequest("GET", fmt.Sprintf("http://localhost:7420/scheduled/%s|%s", ts, jid), nil)
	w := httptest.NewRecorder()
	scheduledJobHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestMorgue(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/morgue", nil)
	assert.Nil(t, err)

	str := defaultServer.Store()
	q := str.Dead()
	q.Clear()
	jid, data := fakeJob()

	err = q.AddElement(util.Nows(), jid, data)
	assert.Nil(t, err)

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
	assert.Nil(t, err)

	req := httptest.NewRequest("GET", fmt.Sprintf("http://localhost:7420/morgue/%s|%s", ts, jid), nil)
	w := httptest.NewRecorder()
	deadHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func TestBusy(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:7420/busy", nil)
	assert.Nil(t, err)

	wid := "1239123oim,bnsad"
	wrk := &server.ClientWorker{
		Hostname:  "foobar.local",
		Pid:       12345,
		Wid:       wid,
		Labels:    []string{"bubba"},
		StartedAt: time.Now(),
	}
	defaultServer.Heartbeats()[wid] = wrk

	w := httptest.NewRecorder()
	busyHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), wid), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "foobar.local"), w.Body.String())
	assert.True(t, strings.Contains(w.Body.String(), "bubba"), w.Body.String())
	assert.False(t, wrk.Quiet())

	data := url.Values{
		"signal": {"quiet"},
		"wid":    {wid},
	}
	req = httptest.NewRequest("POST", "http://localhost:7420/busy", strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	busyHandler(w, req)
	assert.Equal(t, 302, w.Code)
	assert.True(t, wrk.Quiet())
}
