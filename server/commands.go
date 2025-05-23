package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

// A command responds to an client request.
// Each command must parse the request payload (if any), invoke a action and produce a response.
// Commands should not have business logic.
type command func(c *Connection, s *Server, cmd string)

var CommandSet = map[string]command{
	"END":    end,
	"PUSH":   push,
	"PUSHB":  pushBulk,
	"FETCH":  fetch,
	"ACK":    ack,
	"FAIL":   fail,
	"BEAT":   heartbeat,
	"INFO":   info,
	"FLUSH":  flush,
	"MUTATE": mutate,
	"BATCH":  batch,
	"TRACK":  track,
	"QUEUE":  queue,
}

func track(c *Connection, s *Server, cmd string) {
	_ = c.Error(cmd, fmt.Errorf("tracking subsystem is only available in Faktory Enterprise"))
}

func batch(c *Connection, s *Server, cmd string) {
	_ = c.Error(cmd, fmt.Errorf("batch subsystem is only available in Faktory Enterprise"))
}

// QUEUE PAUSE foo bar baz
// QUEUE RESUME *
// QUEUE REMOVE [names...]
func queue(c *Connection, s *Server, cmd string) {
	qs := strings.Split(cmd, " ")[1:]
	subcmd := strings.ToUpper(qs[0])
	ctx := c.Context
	m := s.Manager()
	var op func(ctx context.Context, qName string) error

	switch subcmd {
	case "LATENCY":
		queueLatency(c, s, cmd, qs[1:])
		return
	case "PAUSE":
		op = m.PauseQueue
	case "RESUME":
		op = m.ResumeQueue
	case "REMOVE":
		op = m.RemoveQueue
	}

	if op != nil {
		if qs[1] == "*" {
			s.Store().EachQueue(ctx, func(q storage.Queue) {
				_ = op(ctx, q.Name())
			})
		} else {
			names := qs[1:]
			for idx := range names {
				_ = op(ctx, names[idx])
			}
		}
	} else {
		_ = c.Error(cmd, fmt.Errorf("no such QUEUE subcommand: %s", subcmd))
		return
	}
	_ = c.Ok()
}

func queueLatency(c *Connection, s *Server, cmd string, names []string) {
	if len(names) == 1 && names[0] == "*" {
		_ = c.Error(cmd, fmt.Errorf("QUEUE LATENCY does not support wildcards"))
		return
	}

	times, err := gatherLatencies(c.Context, names, s.Store())
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("QUEUE: %w", err))
		return
	}
	res, err := json.Marshal(times)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("QUEUE: %w", err))
		return
	}

	_ = c.Result(res)
}

func gatherLatencies(ctx context.Context, qs []string, store storage.Store) (map[string]float64, error) {
	queueCmd := map[string]*redis.StringCmd{}
	_, err := store.Redis().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, q := range qs {
			queueCmd[q] = pipe.LIndex(ctx, q, -1)
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		util.Error("Unable to gather queue latencies", err)
		return nil, err
	}

	result := map[string]float64{}
	for name, lindex := range queueCmd {
		latency := 0.0
		payload := lindex.Val()
		if payload != "" {
			var job client.Job
			err := json.Unmarshal([]byte(payload), &job)
			if err != nil {
				return nil, err
			} else {
				tm, err := util.ParseTime(job.EnqueuedAt)
				if err != nil {
					return nil, err
				} else {
					latency = float64(time.Since(tm)) / float64(time.Second)
				}
			}
			result[name] = latency
		} else {
			result[name] = 0
		}
	}
	return result, nil
}

// FLUSH
func flush(c *Connection, s *Server, cmd string) {
	if s.Options.Environment == "development" {
		util.Info("Flushing dataset")
	} else {
		util.Warn("Flushing dataset")
	}
	err := s.store.Flush(c.Context)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

// END
func end(c *Connection, s *Server, cmd string) {
	c.Close()
}

// PUSHB [{job},{job},{job},...] => Map<JID, Error>
func pushBulk(c *Connection, s *Server, cmd string) {
	data := cmd[6:]
	jobs := make([]client.Job, 0)

	err := util.JsonUnmarshal([]byte(data), &jobs)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid JSON: %w", err))
		return
	}

	result := map[string]string{}
	ts := util.Nows()

	for idx := range jobs {
		job := jobs[idx]
		// caller can leave out the CreatedAt element
		if job.CreatedAt == "" {
			job.CreatedAt = ts
		}
		if job.Retry == nil {
			// If retry is not set, we want to use the default policy
			job.Retry = &client.RetryPolicyDefault
		}
		// TODO we aren't optimizing the roundtrips to Redis yet
		// We need a new `manager.PushBulk` API
		err = s.manager.Push(c.Context, &job)
		if err != nil {
			result[job.Jid] = err.Error()
		}
	}

	if len(result) == 0 {
		_ = c.Result([]byte("{}"))
		return
	}
	res, err := json.Marshal(result)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("PUSHB: %w", err))
		return
	}

	_ = c.Result(res)
}

// PUSH {json}
func push(c *Connection, s *Server, cmd string) {
	data := cmd[5:]

	var job client.Job
	job.Retry = &client.RetryPolicyDefault

	err := util.JsonUnmarshal([]byte(data), &job)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	if job.Retry == nil {
		// If retry is not set, we want to use the default policy
		job.Retry = &client.RetryPolicyDefault
	}

	err = s.manager.Push(c.Context, &job)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

// FETCH critical default bulk
func fetch(c *Connection, s *Server, cmd string) {
	if c.client.state != Running {
		// quiet or terminated workers should not get new jobs
		time.Sleep(2 * time.Second)
		_ = c.Result(nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	qs := strings.Split(cmd, " ")[1:]
	job, err := s.manager.Fetch(ctx, c.client.Wid, qs...)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}
	if job != nil {
		res, err := json.Marshal(job)
		if err != nil {
			_ = c.Error(cmd, err)
			return
		}
		_ = c.Result(res)
	} else {
		_ = c.Result(nil)
	}
}

// ACK {"jid":"123456789"}
func ack(c *Connection, s *Server, cmd string) {
	data := cmd[4:]

	var hash map[string]string
	err := util.JsonUnmarshal([]byte(data), &hash)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid ACK %s", data))
		return
	}
	jid, ok := hash["jid"]
	if !ok {
		_ = c.Error(cmd, fmt.Errorf("invalid ACK %s", data))
		return
	}
	_, err = s.manager.Acknowledge(c.Context, jid)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

// FAIL {"jid":"123456789","errmsg":"RuntimeError: blah","backtrace":["line1","line2"]}
func fail(c *Connection, s *Server, cmd string) {
	data := cmd[5:]

	var failure manager.FailPayload
	err := util.JsonUnmarshal([]byte(data), &failure)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid FAIL %s", data))
		return
	}

	err = s.manager.Fail(c.Context, &failure)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}
	_ = c.Ok()
}

// INFO
func info(c *Connection, s *Server, cmd string) {
	data, err := s.CurrentState()
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Result(bytes)
}

type ClientBeat struct {
	CurrentState string `json:"current_state"`
	Wid          string `json:"wid"`
	RssKb        int64  `json:"rss_kb"`
}

// BEAT {"wid":"12345abcde","rss_kb":54176}
func heartbeat(c *Connection, s *Server, cmd string) {
	data := cmd[5:]

	var beat ClientBeat
	err := util.JsonUnmarshal([]byte(data), &beat)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid BEAT %s", data))
		return
	}

	worker, ok := s.workers.heartbeat(&beat)
	if !ok {
		_ = c.Error(cmd, fmt.Errorf("unknown worker %s", beat.Wid))
		return
	}

	if worker.state == Running {
		_ = c.Ok()
	} else {
		_ = c.Result(fmt.Appendf(nil, `{"state":%q}`, stateString(worker.state)))
	}
}
