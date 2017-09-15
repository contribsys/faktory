package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/util"
)

type command func(c *Connection, s *Server, cmd string)

var cmdSet = map[string]command{
	"END":   end,
	"PUSH":  push,
	"FETCH": fetch,
	"ACK":   ack,
	"FAIL":  fail,
	"BEAT":  heartbeat,
	"INFO":  info,
	"STORE": store,
}

func end(c *Connection, s *Server, cmd string) {
	c.Close()
}

func push(c *Connection, s *Server, cmd string) {
	data := []byte(cmd[5:])
	job, err := parseJob(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	if job.At != "" {
		t, err := util.ParseTime(job.At)
		if err != nil {
			c.Error(cmd, fmt.Errorf("Invalid timestamp for job.at: %s", job.At))
			return
		}

		if t.After(time.Now()) {
			data, err = json.Marshal(job)
			if err != nil {
				c.Error(cmd, err)
				return
			}
			// scheduler for later
			err = s.store.Scheduled().AddElement(job.At, job.Jid, data)
			if err != nil {
				c.Error(cmd, err)
				return
			}
			c.Ok()
			return
		}
	}

	// enqueue immediately
	q, err := s.store.GetQueue(job.Queue)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	job.EnqueuedAt = util.Nows()
	data, err = json.Marshal(job)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	err = q.Push(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
}

func fetch(c *Connection, s *Server, cmd string) {
	// quiet or terminated clients should not get new jobs
	if c.client.state != "" {
		c.Result(nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qs := strings.Split(cmd, " ")[1:]
	job, err := s.Fetch(func(job *faktory.Job) error {
		return s.Reserve(c.client.Wid, job)
	}, ctx, qs...)
	if err != nil {
		c.Error(cmd, err)
		return
	}
	if job != nil {
		res, err := json.Marshal(job)
		if err != nil {
			c.Error(cmd, err)
			return
		}
		atomic.AddInt64(&s.Processed, 1)
		c.Result(res)
	} else {
		c.Result(nil)
	}
}

func ack(c *Connection, s *Server, cmd string) {
	jid := cmd[4:]
	_, err := s.Acknowledge(jid)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
}

func info(c *Connection, s *Server, cmd string) {
	defalt, err := s.store.GetQueue("default")
	if err != nil {
		c.Error(cmd, err)
		return
	}
	fmt.Printf("%+v\n", s)
	data := map[string]interface{}{
		"failures":  s.Failures,
		"processed": s.Processed,
		"working":   s.scheduler.Working.Stats(),
		"retries":   s.scheduler.Retries.Stats(),
		"scheduled": s.scheduler.Scheduled.Stats(),
		"default":   defalt.Size(),
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Result(bytes)
}

func store(c *Connection, s *Server, cmd string) {
	subcmd := strings.ToLower(strings.Split(cmd, " ")[1])
	switch subcmd {
	case "stats":
		c.Result([]byte(s.store.Stats()["stats"]))
	case "backup":
		// TODO
	default:
		c.Error(cmd, fmt.Errorf("Unknown STORE command: %s", subcmd))
	}
}

/*
BEAT {"wid":1238971623}
*/
func heartbeat(c *Connection, s *Server, cmd string) {
	if !strings.HasPrefix(cmd, "BEAT {") {
		c.Error(cmd, fmt.Errorf("Invalid format %s", cmd))
		return
	}

	var worker ClientWorker
	data := cmd[5:]
	err := json.Unmarshal([]byte(data), &worker)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid format %s", data))
		return
	}

	entry, ok := s.heartbeats[worker.Wid]
	if !ok {
		c.Error(cmd, fmt.Errorf("Unknown client %d", worker.Wid))
		return
	}

	entry.lastHeartbeat = time.Now()

	if entry.signal == "" {
		c.Ok()
	} else {
		c.Result([]byte(fmt.Sprintf(`{"signal":"%s"}`, entry.signal)))
		entry.signal = ""
	}
}
