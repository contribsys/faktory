package webui

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/justinas/nosurf"
	redis "github.com/redis/go-redis/v9"
)

func Render(s string, w http.ResponseWriter, pp *PageData) {
	fmt.Printf("Rendering %s\n", s)
	tmpl, err := pp.Context().webui.Template(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tmpl.ExecuteTemplate(w, "layout", pp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type PageData struct {
	Request     *http.Request
	CurrentPage uint64
	Count       uint64
	TotalSize   uint64
	Element     interface{}
	Key         string
}

func (pd *PageData) CurrentStatus() string {
	if pd.Context().Server().Manager().WorkingCount() == 0 {
		return "idle"
	}
	return "active"
}

func (d *PageData) CsrfTag() template.HTML {
	if d.Context().UseCsrf() {
		return template.HTML(`<input type="hidden" name="csrf_token" value="` + nosurf.Token(d.Request) + `"/>`)
	} else {
		return ""
	}
}

func (pc *PageData) Tabs() []Tab {
	return DefaultTabs
}

func (pc *PageData) Root() string {
	return pc.Context().Root
}

func (pc *PageData) T(key string) string {
	return pc.Context().Translation(key)
}

func (pc *PageData) LicenseStatus() template.HTML {
	return template.HTML(LicenseStatus(pc.Request)) // nolint:gosec
}

func (pc *PageData) CurrentTime() string {
	return time.Now().UTC().Format("15:04:05 UTC")
}

func (pc *PageData) Context() *DefaultContext {
	return pc.Request.Context().(*DefaultContext)
}

func (pc *PageData) Parameter(name string) string {
	return pc.Request.URL.Query().Get(name)
}

func (pc *PageData) Name() string {
	return client.Name
}

func (pc *PageData) Version() string {
	return client.Version
}

func (pc *PageData) Connections() uint64 {
	return pc.Context().Server().Stats.Connections
}

func (pc *PageData) CommandsExecuted() uint64 {
	return pc.Context().Server().Stats.Commands
}

func (pc *PageData) MemoryUsage() uint64 {
	return util.MemoryUsageMB()
}

func (pc *PageData) Uptime() string {
	return fmt.Sprintf("%.0f", time.Since(pc.Context().Server().Stats.StartedAt).Seconds()/float64(86400))
}

func (pc *PageData) Job() *client.Job {
	return pc.Element.(*client.Job)
}

func (pc *PageData) PreviousPage() uint64 {
	return pc.CurrentPage - 1
}

func (pc *PageData) NextPage() uint64 {
	return pc.CurrentPage + 1
}

func (pc *PageData) TotalSoFar() uint64 {
	return pc.CurrentPage * pc.Count
}

func (pc *PageData) LastPage() uint64 {
	return uint64(math.Ceil(float64(pc.TotalSize) / float64(pc.Count)))
}

func (pc *PageData) BusyReservations() ([]*manager.Reservation, error) {
	c := pc.Context().Context
	results := make([]*manager.Reservation, 0)
	err := pc.Context().Store().Working().Each(c, func(_ int, entry storage.SortedEntry) error {
		var res manager.Reservation
		err := json.Unmarshal(entry.Value(), &res)
		if err != nil {
			return err
		}
		results = append(results, &res)
		return nil
	})
	return results, err
}

func (pc *PageData) BusyWorkers() ([]*server.ClientData, error) {
	hb := pc.Context().Server().Heartbeats()
	results := make([]*server.ClientData, 0)
	wids := make([]string, len(hb))
	idx := 0
	for wid := range hb {
		wids[idx] = wid
		idx++
	}
	sort.Strings(wids)
	for idx := range wids {
		results = append(results, hb[wids[idx]])
	}
	return results, nil
}

type HistoryJson struct {
	Processed string
	Failed    string
}

func (pc *PageData) History() (result HistoryJson, err error) {
	ctx := pc.Context()
	days := 30
	daystr := pc.Request.URL.Query().Get("days")
	if daystr != "" {
		cnt, err := strconv.Atoi(daystr)
		if err == nil && cnt <= 180 {
			days = cnt
		}
	}
	procd := map[string]uint64{}
	faild := map[string]uint64{}
	err = ctx.Store().History(ctx.Context, days, func(daystr string, p, f uint64) {
		procd[daystr] = p
		faild[daystr] = f
	})
	if err != nil {
		return
	}
	p, err := json.Marshal(procd)
	if err != nil {
		return
	}
	f, err := json.Marshal(faild)
	if err != nil {
		return
	}
	return HistoryJson{Processed: string(p), Failed: string(f)}, nil
}

func (pd *PageData) Metric(name string) string {
	d := pd.Context()
	if name == "processed" {
		return uintWithDelimiter(d.Store().TotalProcessed(d.Context))
	} else if name == "failures" {
		return uintWithDelimiter(d.Store().TotalFailures(d.Context))
	} else if name == "retries" {
		return uintWithDelimiter(d.Store().Retries().Size(d.Context))
	} else if name == "scheduled" {
		return uintWithDelimiter(d.Store().Scheduled().Size(d.Context))
	} else if name == "dead" {
		return uintWithDelimiter(d.Store().Dead().Size(d.Context))
	} else if name == "working" {
		return uintWithDelimiter(d.Store().Working().Size(d.Context))
	} else if name == "enqueued" {
		ctx := d.Context
		s := d.Store()
		queueCmd := map[string]*redis.IntCmd{}
		_, err := s.Redis().Pipelined(ctx, func(pipe redis.Pipeliner) error {
			s.EachQueue(ctx, func(q storage.Queue) {
				queueCmd[q.Name()] = pipe.LLen(ctx, q.Name())
			})
			return nil
		})
		if err != nil {
			util.Error("Error talking to Redis", err)
			return "-0"
		}

		totalQueued := uint64(0)
		for _, cmd := range queueCmd {
			totalQueued += uint64(cmd.Val())
		}
		return uintWithDelimiter(totalQueued)
	} else {
		panic(name)
	}
}
