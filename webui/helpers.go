package webui

import (
	"net/http"
	"strconv"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/storage"
)

var (
	utcFormat = "15:04:05 UTC"
)

func serverUtcTime() string {
	return time.Now().UTC().Format(utcFormat)
}

func productVersion() string {
	return faktory.Version
}

func serverLocation() string {
	return defaultServer.Options.Binding
}

func t(word string) string {
	return word
}

func currentStatus() string {
	if defaultServer.Store().Working().Size() == 0 {
		return "idle"
	}
	return "active"
}

type Queue struct {
	Name string
	Size int64
}

func queues() []Queue {
	queues := make([]Queue, 0)
	defaultServer.Store().EachQueue(func(q storage.Queue) {
		queues = append(queues, Queue{q.Name(), q.Size()})
	})
	return queues
}

func csrfTag(req *http.Request) string {
	// random string :-)
	return `<input type="hidden" name="authenticity_token" value="p8tNCpaxTOdAEgoTT3UdSzReVPdWTRJimHS8zDXAVPw="/>`
}

func numberWithDelimiter(val int64) string {
	in := strconv.FormatInt(val, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}
