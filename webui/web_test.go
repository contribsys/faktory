package webui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/storage"
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

	q.AddElement(util.Nows(), jid, data)

	w := httptest.NewRecorder()
	retriesHandler(w, req)
	assert.Equal(t, 200, w.Code)
	assert.True(t, strings.Contains(w.Body.String(), jid), w.Body.String())
}

func init() {
	storage.DefaultPath = "../tmp"
	bootRuntime()
}

func bootRuntime() *server.Server {
	s := server.NewServer(&server.ServerOptions{Binding: "localhost:7418"})
	go func() {
		err := s.Start()
		if err != nil {
			panic(err.Error())
		}
	}()

	defaultServer = s
	for {
		if defaultServer.Store() != nil {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	return s
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	nows := util.Nows()
	return jid, []byte(fmt.Sprintf(`{
			"jid":"%s",
			"created_at":"%s",
			"queue":"default",
			"args":[1,2,3],
			"jobtype":"SomeWorker",
			"enqueued_at":"%s",
			"failure":{
				"retry_count":0,
				"failed_at":"%s",
				"message":"Invalid argument",
				"errtype":"RuntimeError"
			},
			"custom":{
				"foo":"bar",
				"tenant":1
			}
		}`, jid, nows, nows, nows))
}
