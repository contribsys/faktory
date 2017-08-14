package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/storage"
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
	req, err := http.NewRequest("GET", "http://localhost:7420/", nil)
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
