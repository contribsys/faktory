package webui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/justinas/nosurf"
)

var (
	utcFormat = "15:04:05 UTC"
)

func productTitle(req *http.Request) string {
	return ctx(req).webui.Title
}

func extraCss(req *http.Request) string {
	url := ctx(req).webui.ExtraCssUrl
	if url != "" && strings.HasPrefix(url, "http") {
		return fmt.Sprintf("<link href='%s' media='screen' rel='stylesheet' type='text/css'/>", url)
	}
	return ""
}

func root(req *http.Request) string {
	return ctx(req).Root
}

func relative(req *http.Request, relpath string) string {
	return fmt.Sprintf("%s%s", root(req), relpath)
}

func serverUtcTime() string {
	return time.Now().UTC().Format(utcFormat)
}

func serverLocation(req *http.Request) string {
	return ctx(req).Server().Options.Binding
}

func rtl(req *http.Request) bool {
	return t(req, "TextDirection") == "rtl"
}

func textDir(req *http.Request) string {
	dir := t(req, "TextDirection")
	if dir == "" || dir == "TextDirection" {
		dir = "ltr"
	}
	return dir
}

func t(req *http.Request, word string) string {
	dc := req.Context().(*DefaultContext)
	return dc.Translation(word)
}

func pageparam(req *http.Request, pageValue uint64) string {
	return fmt.Sprintf("page=%d", pageValue)
}

func currentStatus(req *http.Request) string {
	if ctx(req).Server().Manager().WorkingCount() == 0 {
		return "idle"
	}
	return "active"
}

type Queue struct {
	Name     string
	Size     uint64
	IsPaused bool
}

func queues(req *http.Request) []Queue {
	// ctx := req.Context()
	c := req.Context()
	queues := make([]Queue, 0)
	s := ctx(req).Store()
	pq, _ := s.PausedQueues(c)

	s.EachQueue(c, func(q storage.Queue) {
		paused := false
		for idx := range pq {
			if q.Name() == pq[idx] {
				paused = true
			}
		}
		queues = append(queues, Queue{q.Name(), q.Size(c), paused})
	})

	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})
	return queues
}

func ctx(req *http.Request) *DefaultContext {
	return req.Context().(*DefaultContext)
}

func csrfTag(req *http.Request) string {
	if ctx(req).UseCsrf() {
		return `<input type="hidden" name="csrf_token" value="` + nosurf.Token(req) + `"/>`
	} else {
		return ""
	}
}

func uintWithDelimiter(val uint64) string {
	in := strconv.FormatUint(val, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}

	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		k++
		if k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

func queueJobs(r *http.Request, q storage.Queue, count, currentPage uint64, fn func(idx int, key []byte, job *client.Job)) {
	c := r.Context()
	err := q.Page(c, int64((currentPage-1)*count), int64(count), func(idx int, data []byte) error {
		var job client.Job
		err := json.Unmarshal(data, &job)
		if err != nil {
			util.Warnf("Error parsing JSON: %s", string(data))
			return err
		}
		fn(idx, data, &job)
		return nil
	})
	if err != nil {
		util.Warnf("Error iterating queue: %s", err.Error())
	}
}

func enqueuedSize(req *http.Request) uint64 {
	c := req.Context()
	var total uint64
	ctx(req).Store().EachQueue(c, func(q storage.Queue) {
		total += q.Size(c)
	})
	return total
}

func relativeTime(moment string) string {
	tm, err := util.ParseTime(moment)
	if err != nil {
		return "invalid timestamp"
	}
	return Timeago(tm)
}

func unfiltered() bool {
	return true
}

func filtering(set string) string {
	return ""
}

func setJobs(req *http.Request, set storage.SortedSet, count, currentPage uint64, fn func(idx int, key []byte, job *client.Job)) {
	c := req.Context()
	_, err := set.Page(c, int((currentPage-1)*count), int(count), func(idx int, entry storage.SortedEntry) error {
		job, err := entry.Job()
		if err != nil {
			util.Warnf("Error parsing JSON: %s", string(entry.Value()))
			return err
		}
		key, err := entry.Key()
		if err != nil {
			return err
		}
		fn(idx, key, job)
		return nil
	})
	if err != nil {
		util.Error("Error iterating sorted set", err)
	}
}

func busyReservations(req *http.Request, fn func(worker *manager.Reservation)) {
	c := req.Context()
	err := ctx(req).Store().Working().Each(c, func(idx int, entry storage.SortedEntry) error {
		var res manager.Reservation
		err := json.Unmarshal(entry.Value(), &res)
		if err != nil {
			util.Error("Cannot unmarshal reservation", err)
		} else {
			fn(&res)
		}
		return err
	})
	if err != nil {
		util.Error("Error iterating reservations", err)
	}
}

func busyWorkers(req *http.Request, fn func(proc *server.ClientData)) {
	hb := ctx(req).Server().Heartbeats()
	wids := make([]string, len(hb))
	idx := 0
	for wid := range hb {
		wids[idx] = wid
		idx++
	}
	sort.Strings(wids)
	for idx := range wids {
		fn(hb[wids[idx]])
	}
}

func actOn(req *http.Request, set storage.SortedSet, action string, keys []string) error {
	c := req.Context()
	switch action {
	case "delete":
		if len(keys) == 1 && keys[0] == "all" {
			return set.Clear(c)
		} else {
			for idx := range keys {
				_, err := set.Remove(c, []byte(keys[idx]))
				// ok doesn't really matter
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "add_to_queue", "retry":
		if len(keys) == 1 && keys[0] == "all" {
			return ctx(req).Store().EnqueueAll(c, set)
		} else {
			for idx := range keys {
				err := ctx(req).Store().EnqueueFrom(c, set, []byte(keys[idx]))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "kill":
		if len(keys) == 1 && keys[0] == "all" {
			return ctx(req).Store().EnqueueAll(c, set)
		} else {
			// TODO Make this 180 day dead job expiry dynamic per-job or
			// a global variable in TOML? PRs welcome.
			expiry := time.Now().Add(180 * 24 * time.Hour)
			for idx := range keys {
				entry, err := set.Get(c, []byte(keys[idx]))
				if err != nil {
					return err
				}
				if entry != nil {
					err = set.MoveTo(c, ctx(req).Store().Dead(), entry, expiry)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}
	default:
		return fmt.Errorf("invalid action: %v", action)
	}
}

func uptimeInDays(req *http.Request) string {
	return fmt.Sprintf("%.0f", time.Since(ctx(req).Server().Stats.StartedAt).Seconds()/float64(86400))
}

func displayRss(rssKb int64) string {
	if rssKb < 100000 {
		return strconv.FormatInt(rssKb, 10) + " KB"
	} else if rssKb < 10000000 {
		return strconv.FormatFloat(float64(rssKb)/1024, 'f', 1, 64) + " MB"
	} else {
		return strconv.FormatFloat(float64(rssKb)/(1024*1024), 'f', 1, 64) + " GB"
	}
}

func category_for_rtt(lat float64) string {
	if lat == 0 {
		return "danger"
	} else if lat < 1000 {
		return "success"
	} else if lat < 10000 {
		return "primary"
	} else {
		return "danger"
	}
}

func redis_info(req *http.Request) (string, float64) {
	c := req.Context()
	store := ctx(req).Store().(storage.Redis)
	redis := store.Redis()
	a := time.Now().UnixNano()
	res := redis.Info(c)
	b := time.Now().UnixNano()
	val, err := res.Result()
	if err != nil {
		return fmt.Sprintf("%v", err), 0
	}
	return val, (float64(b-a) / 1000)
}

func days(req *http.Request) int {
	days := req.URL.Query()["days"]
	if len(days) == 0 {
		return 30
	}
	daystr := days[0]
	if daystr == "" {
		return 30
	}
	cnt, err := strconv.Atoi(daystr)
	if err != nil {
		return 30
	}
	if cnt > 180 {
		return 180
	}
	return cnt
}

func daysMatches(req *http.Request, value string, defalt bool) string {
	days := req.URL.Query()["days"]
	daysValue := ""
	if len(days) > 0 {
		daysValue = days[0]
	}
	if daysValue == value {
		return "active"
	}
	if daysValue == "" && defalt {
		return "active"
	}
	return ""
}

func processedHistory(req *http.Request) string {
	c := req.Context()
	cnt := days(req)
	procd := map[string]uint64{}
	// faild := map[string]int64{}

	err := ctx(req).Store().History(c, cnt, func(daystr string, p, f uint64) {
		procd[daystr] = p
		// faild[daystr] = f
	})
	if err != nil {
		return err.Error()
	}
	str, err := json.Marshal(procd)
	if err != nil {
		return err.Error()
	}
	return string(str)
}

func failedHistory(req *http.Request) string {
	c := req.Context()
	cnt := days(req)
	// procd := map[string]int64{}
	faild := map[string]uint64{}

	err := ctx(req).Store().History(c, cnt, func(daystr string, p, f uint64) {
		// procd[daystr] = p
		faild[daystr] = f
	})
	if err != nil {
		return err.Error()
	}
	str, err := json.Marshal(faild)
	if err != nil {
		return err.Error()
	}
	return string(str)
}

func sortedLocaleNames(req *http.Request, fn func(string, bool)) {
	c := ctx(req)
	names := make(sort.StringSlice, len(locales))
	i := 0
	for name := range locales {
		names[i] = name
		i++
	}
	names.Sort()

	for idx := range names {
		fn(names[idx], names[idx] == c.locale)
	}
}

func displayJobType(j *client.Job) string {
	if j.Type == "ActiveJob::QueueAdapters::FaktoryAdapter::JobWrapper" {
		jobClass, ok := j.Custom["wrapped"].(string)
		if ok {
			if jobClass == "ActionMailer::DeliveryJob" || jobClass == "ActionMailer::MailDeliveryJob" {
				args, ok := j.Args[0].(map[string]interface{})
				if ok {
					// Get the actual job arguments
					arguments, ok := args["arguments"].([]interface{})
					if ok {
						if len(arguments) >= 2 {
							mailerClass, ok1 := arguments[0].(string)
							mailerMethod, ok2 := arguments[1].(string)
							if ok1 && ok2 {
								return fmt.Sprintf("%s#%s", mailerClass, mailerMethod)
							}
						}
					}
				}
			}
			return jobClass
		}
	}
	return j.Type
}

func displayArgs(args []interface{}) string {
	return displayLimitedArgs(args, 1024)
}

func displayFullArgs(args []interface{}) string {
	return displayLimitedArgs(args, 1024*1024)
}

func displayLimitedArgs(args []interface{}, limit int) string {
	var b strings.Builder
	for idx := range args {
		var s string
		var data bytes.Buffer
		enc := json.NewEncoder(&data)
		enc.SetEscapeHTML(false)
		err := enc.Encode(args[idx])
		if err != nil {
			util.Warnf("Unable to marshal argument for display: %s", err)
			s = fmt.Sprintf("%#v", args[idx])
		} else {
			s = data.String()
		}
		if len(s) > limit {
			fmt.Fprintf(&b, s[0:limit])
			b.WriteRune('…')
		} else {
			fmt.Fprint(&b, s)
		}
		if idx+1 < len(args) {
			b.WriteRune(',')
			b.WriteRune(' ')
		}
	}
	return b.String()
}
