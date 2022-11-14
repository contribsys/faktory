package webui

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLiveServer(t *testing.T) {
	bootRuntime(t, "webui", func(ui *WebUI, s *server.Server, t *testing.T) {
		t.Run("StaticAssets", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/static/application.js", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			staticHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "Fuzzy"), w.Body.String())
		})

		t.Run("Debug", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/debug", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			debugHandler(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "Disk Usage"), w.Body.String())
		})

		t.Run("Health", func(t *testing.T) {
			req, err := ui.NewRequest("GET", "http://localhost:7420/health", nil)
			assert.NoError(t, err)

			w := httptest.NewRecorder()
			healthHandler(ui)(w, req)
			assert.Equal(t, 200, w.Code)
			assert.True(t, strings.Contains(w.Body.String(), "faktory_version"), w.Body.String())
		})

		t.Run("ComputeLocale", func(t *testing.T) {
			lang := localeFromHeader("")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader(" 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4,ru;q=0.2'")
			assert.Equal(t, "fr", lang)
			lang = localeFromHeader("zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,ru;q=0.2")
			assert.Equal(t, "zh-cn", lang)
			lang = localeFromHeader("en-US,sv-SE;q=0.8,sv;q=0.6,en;q=0.4")
			assert.Equal(t, "sv", lang)
			lang = localeFromHeader("nb-NO,nb;q=0.2")
			assert.Equal(t, "nb", lang)
			lang = localeFromHeader("en-us")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("sv-se")
			assert.Equal(t, "sv", lang)
			lang = localeFromHeader("pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4")
			assert.Equal(t, "pt-br", lang)
			lang = localeFromHeader("pt-PT,pt;q=0.8,en-US;q=0.6,en;q=0.4")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("pt-br")
			assert.Equal(t, "pt-br", lang)
			lang = localeFromHeader("pt-pt")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("pt")
			assert.Equal(t, "pt", lang)
			lang = localeFromHeader("en-us; *")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("en-US,en;q=0.8")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("en-GB,en-US;q=0.8,en;q=0.6")
			assert.Equal(t, "en", lang)
			lang = localeFromHeader("ru,en")
			assert.Equal(t, "ru", lang)
			lang = localeFromHeader("*")
			assert.Equal(t, "en", lang)
		})
	})
}

func bootRuntime(t *testing.T, name string, fn func(*WebUI, *server.Server, *testing.T)) {
	dir := fmt.Sprintf("/tmp/faktory-test-%s", name)
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/redis.sock", dir)
	stopper, err := storage.Boot(dir, sock)
	if err != nil {
		panic(err)
	}
	defer stopper()

	s, err := server.NewServer(&server.ServerOptions{
		Binding:          "localhost:7418",
		StorageDirectory: dir,
		RedisSock:        sock,
	})

	if err != nil {
		panic(err)
	}
	err = s.Boot()
	if err != nil {
		panic(err)
	}
	defer s.Stop(nil)
	s.Store().Flush(context.Background())

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	web := newWeb(s, defaultOptions())

	fn(web, s, t)
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
		"at":"%s",
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
	}`, jid, nows, nows, nows, nows))
}
