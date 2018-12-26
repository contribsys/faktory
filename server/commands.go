package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

// A command responds to an client request.
// Each command must parse the request payload (if any), invoke a action and produce a response.
// Commands should not have business logic.
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
	data := cmd[5:]

	var job client.Job
	err := json.Unmarshal([]byte(data), &job)
	if err != nil {
		c.Error(cmd, newTaggedError("MALFORMED", err))
		return
	}

	err = s.manager.Push(&job)
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

	c.Ok()
}

func fail(c *Connection, s *Server, cmd string) {
	data := cmd[5:]

	var failure manager.FailPayload
	err := json.Unmarshal([]byte(data), &failure)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid FAIL %s", data))
		return
	}

	err = s.manager.Fail(&failure)
	if err != nil {
		c.Error(cmd, err)
		return
	}
	c.Ok()
}

func info(c *Connection, s *Server, cmd string) {
	data, err := s.CurrentState()
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

func heartbeat(c *Connection, s *Server, cmd string) {
	data := cmd[5:]

	var client ClientData
	err := json.Unmarshal([]byte(data), &client)
	if err != nil {
		c.Error(cmd, fmt.Errorf("Invalid BEAT %s", data))
		return
	}

	worker, ok := s.workers.heartbeat(&client, nil)
	if !ok {
		c.Error(cmd, fmt.Errorf("Unknown worker %s", client.Wid))
		return
	}

	if worker.state == Running {
		c.Ok()
	} else {
		c.Result([]byte(fmt.Sprintf(`{"state":"%s"}`, stateString(worker.state))))
	}
}
