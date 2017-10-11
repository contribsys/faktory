package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/storage"
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

func uptimeInDays(s *Server) string {
	return fmt.Sprintf("%.0f", time.Now().Sub(s.Stats.StartedAt).Seconds()/float64(86400))
}

func currentMemoryUsage(s *Server) string {
	// TODO maybe remove this and/or offer a better stat?
	return "123 MB"
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
			"total_failures":  s.Stats.Failures,
			"total_processed": s.Stats.Processed,
			"total_enqueued":  totalQueued,
			"total_queues":    totalQueues,
			"tasks":           s.taskRunner.Stats()},
		"server": map[string]interface{}{
			"faktory_version": faktory.Version,
			"uptime_in_days":  uptimeInDays(s),
			"connections":     s.Stats.Connections,
			"command_count":   s.Stats.Commands,
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

	var worker ClientWorker
	data := cmd[5:]
	err := json.Unmarshal([]byte(data), &worker)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid format %s", data))
		return
	}

	entry, ok := s.heartbeats[worker.Wid]
	if !ok {
		c.Error(cmd, fmt.Errorf("Unknown client %s", worker.Wid))
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
