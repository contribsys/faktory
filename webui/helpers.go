package webui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	if dir == "" {
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
	Name string
	Size uint64
}

func queues(req *http.Request) []Queue {
	queues := make([]Queue, 0)
	ctx(req).Store().EachQueue(func(q storage.Queue) {
		queues = append(queues, Queue{q.Name(), q.Size()})
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
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

func queueJobs(q storage.Queue, count, currentPage uint64, fn func(idx int, key []byte, job *client.Job)) {
	err := q.Page(int64((currentPage-1)*count), int64(count), func(idx int, data []byte) error {
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
	var total uint64
	ctx(req).Store().EachQueue(func(q storage.Queue) {
		total += q.Size()
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

func setJobs(set storage.SortedSet, count, currentPage uint64, fn func(idx int, key []byte, job *client.Job)) {
	_, err := set.Page(int((currentPage-1)*count), int(count), func(idx int, entry storage.SortedEntry) error {
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
	err := ctx(req).Store().Working().Each(func(idx int, entry storage.SortedEntry) error {
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
	for _, worker := range ctx(req).Server().Heartbeats() {
		fn(worker)
	}
}

func actOn(req *http.Request, set storage.SortedSet, action string, keys []string) error {
	switch action {
	case "delete":
		if len(keys) == 1 && keys[0] == "all" {
			return set.Clear()
		} else {
			for _, key := range keys {
				_, err := set.Remove([]byte(key))
				// ok doesn't really matter
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "add_to_queue", "retry":
		if len(keys) == 1 && keys[0] == "all" {
			return ctx(req).Store().EnqueueAll(set)
		} else {
			for _, key := range keys {
				err := ctx(req).Store().EnqueueFrom(set, []byte(key))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "kill":
		if len(keys) == 1 && keys[0] == "all" {
			return ctx(req).Store().EnqueueAll(set)
		} else {
			// TODO Make this 180 day dead job expiry dynamic per-job or
			// a global variable in TOML? PRs welcome.
			expiry := time.Now().Add(180 * 24 * time.Hour)
			for _, key := range keys {
				entry, err := set.Get([]byte(key))
				if err != nil {
					return err
				}
				if entry != nil {
					err = set.MoveTo(ctx(req).Store().Dead(), entry, expiry)
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

func redis_info(req *http.Request) string {
	client := ctx(req).Store().(storage.Redis)
	val, err := client.Redis().Info().Result()
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	return val
}
func rss() string {
	ex, err := util.FileExists("/proc/self/status")
	if err != nil || !ex {
		return ""
	}

	content, err := ioutil.ReadFile("/proc/self/status")
	if err != nil {
		return ""
	}

	lines := bytes.Split(content, []byte("\n"))
	for line := range lines {
		ls := string(line)
		if strings.Contains(ls, "VmRSS") {
			return strings.Split(ls, ":")[1]
		}
	}
	return ""
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
	cnt := days(req)
	procd := map[string]uint64{}
	//faild := map[string]int64{}

	err := ctx(req).Store().History(cnt, func(daystr string, p, f uint64) {
		procd[daystr] = p
		//faild[daystr] = f
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
	cnt := days(req)
	//procd := map[string]int64{}
	faild := map[string]uint64{}

	err := ctx(req).Store().History(cnt, func(daystr string, p, f uint64) {
		//procd[daystr] = p
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

	for _, name := range names {
		fn(name, name == c.locale)
	}
}

func displayArgs(args []interface{}) string {
	return displayLimitedArgs(args, 1024)
}

func displayFullArgs(args []interface{}) string {
	return displayLimitedArgs(args, 1024*1024)
}

func displayLimitedArgs(args []interface{}, limit int) string {
	var b strings.Builder
	for idx, arg := range args {
		var s string
		bytes, err := json.Marshal(arg)
		if err != nil {
			util.Warnf("Unable to marshal argument for display: %s", err)
			s = fmt.Sprintf("%#v", arg)
		} else {
			s = string(bytes)
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
