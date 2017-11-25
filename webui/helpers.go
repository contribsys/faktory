package webui

import (
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
)

var (
	utcFormat = "15:04:05 UTC"
)

func serverUtcTime() string {
	return time.Now().UTC().Format(utcFormat)
}

func serverLocation() string {
	return defaultServer.Options.Binding
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

func currentStatus() string {
	if defaultServer.Store().Working().Size() == 0 {
		return "idle"
	}
	return "active"
}

type Queue struct {
	Name string
	Size uint64
}

func queues() []Queue {
	queues := make([]Queue, 0)
	defaultServer.Store().EachQueue(func(q storage.Queue) {
		queues = append(queues, Queue{q.Name(), q.Size()})
	})
	return queues
}

func store() storage.Store {
	return defaultServer.Store()
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

func queueJobs(q storage.Queue, count, currentPage uint64, fn func(idx int, key []byte, job *client.Job)) {
	err := q.Page(int64((currentPage-1)*count), int64(count), func(idx int, key, data []byte) error {
		var job client.Job
		err := json.Unmarshal(data, &job)
		if err != nil {
			util.Warnf("Error parsing JSON: %s", string(data))
			return err
		}
		fn(idx, key, &job)
		return nil
	})
	if err != nil {
		util.Warnf("Error iterating queue: %s", err.Error())
	}
}

func enqueuedSize() uint64 {
	var total uint64
	defaultServer.Store().EachQueue(func(q storage.Queue) {
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
	err := set.Page(int64((currentPage-1)*count), int64(count), func(idx int, key []byte, data []byte) error {
		var job client.Job
		err := json.Unmarshal(data, &job)
		if err != nil {
			util.Warnf("Error parsing JSON: %s", string(data))
			return err
		}
		fn(idx, key, &job)
		return nil
	})
	if err != nil {
		util.Error("Error iterating sorted set", err)
	}
}

func busyReservations(fn func(worker *manager.Reservation)) {
	err := defaultServer.Store().Working().Each(func(idx int, key []byte, data []byte) error {
		var res manager.Reservation
		err := json.Unmarshal(data, &res)
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

func busyWorkers(fn func(proc *server.ClientData)) {
	for _, worker := range defaultServer.Heartbeats() {
		fn(worker)
	}
}

func actOn(set storage.SortedSet, action string, keys []string) error {
	switch action {
	case "delete":
		if len(keys) == 1 && keys[0] == "all" {
			_, err := set.Clear()
			return err
		} else {
			for _, key := range keys {
				err := set.Remove([]byte(key))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "retry":
		if len(keys) == 1 && keys[0] == "all" {
			return defaultServer.Store().EnqueueAll(set)
		} else {
			for _, key := range keys {
				err := defaultServer.Store().EnqueueFrom(set, []byte(key))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "kill":
		if len(keys) == 1 && keys[0] == "all" {
			return defaultServer.Store().EnqueueAll(set)
		} else {
			expiry := util.Thens(time.Now().Add(180 * 24 * time.Hour))
			for _, key := range keys {
				elms := strings.Split(key, "|")
				err := set.MoveTo(defaultServer.Store().Dead(), elms[0], elms[1], func(data []byte) (string, []byte, error) {
					return expiry, data, nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		}
	default:
		return fmt.Errorf("invalid action: %v", action)
	}
}

func uptimeInDays() string {
	return fmt.Sprintf("%.0f", time.Since(defaultServer.Stats.StartedAt).Seconds()/float64(86400))
}

func locale(req *http.Request) string {
	t, ok := req.Context().(Translator)
	if ok {
		return t.Locale()
	}
	return "en"
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
	procd := map[string]int64{}
	//faild := map[string]int64{}

	defaultServer.Store().History(cnt, func(daystr string, p, f int64) {
		procd[daystr] = p
		//faild[daystr] = f
	})
	str, err := json.Marshal(procd)
	if err != nil {
		return err.Error()
	}
	return string(str)
}

func failedHistory(req *http.Request) string {
	cnt := days(req)
	//procd := map[string]int64{}
	faild := map[string]int64{}

	defaultServer.Store().History(cnt, func(daystr string, p, f int64) {
		//procd[daystr] = p
		faild[daystr] = f
	})
	str, err := json.Marshal(faild)
	if err != nil {
		return err.Error()
	}
	return string(str)
}

func sortedLocaleNames(locales map[string]map[string]string) []string {
	names := make(sort.StringSlice, len(locales))
	i := 0
	for locale, _ := range locales {
		names[i] = locale
		i++
	}
	names.Sort()
	return names
}
