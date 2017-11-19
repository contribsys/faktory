package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
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

	err = s.manager.Push(job)
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
	job, err := s.manager.Fetch(ctx, c.client.Wid, qs...)
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
	_, err = s.manager.Acknowledge(jid)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	s.store.Success()
	c.Ok()
}

func fail(c *Connection, s *Server, cmd string) {
	data := []byte(cmd[5:])

	var failure manager.FailPayload
	err := json.Unmarshal(data, &failure)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	err = s.manager.Fail(&failure)
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
			"total_failures":  store.Failures(),
			"total_processed": store.Processed(),
			"total_enqueued":  totalQueued,
			"total_queues":    totalQueues,
			"tasks":           s.taskRunner.Stats()},
		"server": map[string]interface{}{
			"faktory_version": client.Version,
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
