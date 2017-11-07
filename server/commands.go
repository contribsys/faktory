package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
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
	"FLUSH": flush,
}

func flush(c *Connection, s *Server, cmd string) {
	if s.Options.Environment == "development" {
		util.Info("Flushing dataset")
	} else {
		util.Warn("Flushing dataset")
	}
	err := s.store.Flush()
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
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
	if job.Jid == "" || len(job.Jid) < 8 {
		c.Error(cmd, fmt.Errorf("All jobs must have a reasonable jid parameter"))
		return
	}
	if job.Type == "" {
		c.Error(cmd, fmt.Errorf("All jobs must have a jobtype parameter"))
		return
	}
	if job.Args == nil {
		c.Error(cmd, fmt.Errorf("All jobs must have an args parameter"))
		return
	}

	job.EnsureValidPriority()

	if job.At != "" {
		t, err := util.ParseTime(job.At)
		if err != nil {
			c.Error(cmd, fmt.Errorf("Invalid timestamp for 'at': '%s'", job.At))
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

	err = q.Push(job.GetPriority(), data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
}

func fetch(c *Connection, s *Server, cmd string) {
	if c.client.state != Running {
		// quiet or terminated workers should not get new jobs
		time.Sleep(2 * time.Second)
		c.Result(nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qs := strings.Split(cmd, " ")[1:]
	job, err := s.Fetch(func(job *faktory.Job) error {
		return reserve(c.client.Wid, job, s.store.Working())
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
		atomic.AddInt64(&s.Stats.Processed, 1)
		c.Result(res)
	} else {
		c.Result(nil)
	}
}

func ack(c *Connection, s *Server, cmd string) {
	data := cmd[4:]

	var hash map[string]string
	err := json.Unmarshal([]byte(data), &hash)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid ACK %s", data))
		return
	}
	jid, ok := hash["jid"]
	if !ok {
		c.Error(cmd, fmt.Errorf("Invalid ACK %s", data))
		return
	}
	_, err = acknowledge(jid, s.store.Working())
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
}

func uptimeInSeconds(s *Server) int {
	return int(time.Since(s.Stats.StartedAt).Seconds())
}

func currentMemoryUsage(s *Server) string {
	return util.MemoryUsage()
}

func CurrentState(s *Server) (map[string]interface{}, error) {
	defalt, err := s.store.GetQueue("default")
	if err != nil {
		return nil, err
	}
	store := s.Store()
	totalQueued := 0
	totalQueues := 0
	// queue size is cached so this should be very efficient.
	store.EachQueue(func(q storage.Queue) {
		totalQueued += int(q.Size())
		totalQueues += 1
	})

	return map[string]interface{}{
		"server_utc_time": time.Now().UTC().Format("03:04:05 UTC"),
		"faktory": map[string]interface{}{
			"default_size":    defalt.Size(),
			"total_failures":  atomic.LoadInt64(&s.Stats.Failures),
			"total_processed": atomic.LoadInt64(&s.Stats.Processed),
			"total_enqueued":  totalQueued,
			"total_queues":    totalQueues,
			"tasks":           s.taskRunner.Stats()},
		"server": map[string]interface{}{
			"faktory_version": faktory.Version,
			"uptime":          uptimeInSeconds(s),
			"connections":     atomic.LoadInt64(&s.Stats.Connections),
			"command_count":   atomic.LoadInt64(&s.Stats.Commands),
			"used_memory_mb":  currentMemoryUsage(s)},
	}, nil
}

func info(c *Connection, s *Server, cmd string) {
	data, err := CurrentState(s)
	if err != nil {
		c.Error(cmd, err)
		return
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Result(bytes)
}

/*
BEAT {"wid":1238971623}
*/
func heartbeat(c *Connection, s *Server, cmd string) {
	if !strings.HasPrefix(cmd, "BEAT {") {
		c.Error(cmd, fmt.Errorf("Invalid format %s", cmd))
		return
	}

	var worker ClientData
	data := cmd[5:]
	err := json.Unmarshal([]byte(data), &worker)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid format %s", data))
		return
	}

	s.hbmu.Lock()
	defer s.hbmu.Unlock()
	entry, ok := s.heartbeats[worker.Wid]
	if !ok {
		c.Error(cmd, fmt.Errorf("Unknown client %s", worker.Wid))
		return
	}

	entry.lastHeartbeat = time.Now()

	if entry.state == Running {
		c.Ok()
	} else {
		c.Result([]byte(fmt.Sprintf(`{"state":"%s"}`, stateString(entry.state))))
	}
}
