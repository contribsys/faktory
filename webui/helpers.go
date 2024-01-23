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
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	redis "github.com/redis/go-redis/v9"
)

type Queue struct {
	Name     string
	Size     uint64
	IsPaused bool
}

func (pd *PageData) Queues() ([]Queue, error) {
	c := pd.Context()
	queues := make([]Queue, 0)
	s := c.Store()
	pq, _ := s.PausedQueues(c)

	queueCmd := map[string]*redis.IntCmd{}
	_, err := s.Redis().Pipelined(c.Context, func(pipe redis.Pipeliner) error {
		s.EachQueue(c.Context, func(q storage.Queue) {
			queueCmd[q.Name()] = pipe.LLen(c, q.Name())
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	for name, cmd := range queueCmd {
		qsize := cmd.Val()
		paused := false
		for idx := range pq {
			if name == pq[idx] {
				paused = true
			}
		}
		queues = append(queues, Queue{name, uint64(qsize), paused})
	}

	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})
	return queues, nil
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

func startsWith(a, b string) bool {
	return strings.HasPrefix(a, b)
}

type JobRecord struct {
	Key string
	Job *client.Job
}

func (pd *PageData) QueueJobs() ([]JobRecord, error) {
	c := pd.Context()
	q := pd.Element.(storage.Queue)
	jobs := make([]JobRecord, 0)
	err := q.Page(c, int64((pd.CurrentPage-1)*pd.Count), int64(pd.Count), func(idx int, data []byte) error {
		var job client.Job
		err := json.Unmarshal(data, &job)
		if err != nil {
			return err
		}
		jobs = append(jobs, JobRecord{string(data), &job})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func relativeTime(moment string) string {
	tm, err := util.ParseTime(moment)
	if err != nil {
		return "invalid timestamp"
	}
	return Timeago(tm)
}

func (pd *PageData) SetJobs() ([]JobRecord, error) {
	c := pd.Context()
	set := pd.Element.(storage.SortedSet)
	jobs := make([]JobRecord, 0)

	_, err := set.Page(c, int((pd.CurrentPage-1)*pd.Count), int(pd.Count), func(idx int, entry storage.SortedEntry) error {
		job, err := entry.Job()
		if err != nil {
			util.Warnf("Error parsing JSON: %s", string(entry.Value()))
			return err
		}
		key, err := entry.Key()
		if err != nil {
			return err
		}
		jobs = append(jobs, JobRecord{string(key), job})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func actOn(req *http.Request, set storage.SortedSet, action string, keys []string) error {
	c := req.Context().(*DefaultContext)
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
			return c.Store().EnqueueAll(c, set)
		} else {
			for idx := range keys {
				err := c.Store().EnqueueFrom(c, set, []byte(keys[idx]))
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "kill":
		if len(keys) == 1 && keys[0] == "all" {
			return c.Store().EnqueueAll(c, set)
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
					err = set.MoveTo(c, c.Store().Dead(), entry, expiry)
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

func displayRss(rssKb int64) string {
	if rssKb < 100000 {
		return strconv.FormatInt(rssKb, 10) + " KB"
	} else if rssKb < 10000000 {
		return strconv.FormatFloat(float64(rssKb)/1024, 'f', 1, 64) + " MB"
	} else {
		return strconv.FormatFloat(float64(rssKb)/(1024*1024), 'f', 1, 64) + " GB"
	}
}

func categoryForRTT(lat float64) string {
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

// func sortedLocaleNames(req *http.Request, fn func(string, bool)) {
// 	c := req.Context().(*DefaultContext)
// 	names := make(sort.StringSlice, len(locales))
// 	i := 0
// 	for name := range locales {
// 		names[i] = name
// 		i++
// 	}
// 	names.Sort()

// 	for idx := range names {
// 		fn(names[idx], names[idx] == c.locale)
// 	}
// }

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
