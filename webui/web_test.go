package webui

import (
	"fmt"
	"testing"
	"time"

	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/storage"
	"github.com/mperham/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestComputeLocale(t *testing.T) {
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
}

func init() {
	storage.DefaultPath = "/tmp"
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
