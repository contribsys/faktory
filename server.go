package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
)

type ServerOptions struct {
	Binding     string
	StoragePath string
	Password    string
}

type Server struct {
	Options   *ServerOptions
	Processed int64
	Failures  int64
	pwd       string
	listener  net.Listener
	store     storage.Store
}

func NewServer(opts *ServerOptions) *Server {
	if opts.Binding == "" {
		opts.Binding = "localhost:7419"
	}
	if opts.StoragePath == "" {
		opts.StoragePath = fmt.Sprintf("%s.db", strings.Replace(opts.Binding, ":", "_", -1))
	}
	return &Server{Options: opts, pwd: "123456"}
}

func (s *Server) Start() error {
	store, err := storage.Open("rocksdb", s.Options.StoragePath)
	if err != nil {
		return err
	}
	s.store = store
	s.StartScheduler()

	addr, err := net.ResolveTCPAddr("tcp", s.Options.Binding)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		s.processConnection(conn)
	}

	return nil
}

func (s *Server) Stop(f func()) {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.store != nil {
		s.store.Close()
	}

	if f != nil {
		f()
	}
}

func (s *Server) processConnection(conn net.Conn) {
	// operation must complete within 1 second
	conn.SetDeadline(time.Now().Add(1 * time.Second))

	buf := bufio.NewReader(conn)

	// The first line sent upon connection must be:
	//
	// AHOY pwd:<password> other:pair more:etc\n
	line, err := buf.ReadString('\n')
	if err != nil {
		util.Error("Closing connection", err, nil)
		conn.Close()
		return
	}

	valid := strings.HasPrefix(line, "AHOY ")
	if !valid {
		util.Info("Invalid preamble", line)
		util.Info("Need a valid AHOY")
		conn.Close()
		return
	}

	pairs := strings.Split(line, " ")
	var attrs = make(map[string]string, len(pairs)-1)

	for _, pair := range pairs[1:] {
		two := strings.Split(pair, ":")
		if len(two) != 2 {
			util.Info("Invalid pair", pair)
			conn.Close()
			return
		}

		key := strings.ToLower(two[0])
		value := two[1]
		attrs[key] = value
	}

	for key, value := range attrs {
		switch key {
		case "pwd":
			if value != s.pwd {
				util.Info("Invalid password")
				conn.Close()
				return
			}
			attrs["pwd"] = "<redacted>"
		}
	}

	_, err = conn.Write([]byte("OK\n"))
	if err != nil {
		util.Error("Closing connection", err, nil)
		conn.Close()
		return
	}

	id, ok := attrs["id"]
	if !ok {
		id = conn.RemoteAddr().String()
	}
	c := &Connection{
		ident: id,
		conn:  conn,
		buf:   buf,
	}
	go processLines(c, s)
}

type command func(c *Connection, s *Server, cmd string)

var cmdSet = map[string]command{
	"END":  end,
	"PUSH": push,
	"POP":  pop,
	"ACK":  ack,
	"FAIL": fail,
	"INFO": info,
}

func end(c *Connection, s *Server, cmd string) {
	c.Close()
}

func push(c *Connection, s *Server, cmd string) {
	data := []byte(cmd[5:])
	job, err := ParseJob(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}
	qname := job.Queue
	q, err := s.store.GetQueue(qname)
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

func pop(c *Connection, s *Server, cmd string) {
	qs := strings.Split(cmd, " ")[1:]
	job, err := s.Pop(func(job *Job) error {
		return s.Reserve(c.Identity(), job)
	}, qs...)
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
		c.Result([]byte("\n"))
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
	data := map[string]interface{}{
		"failures":  s.Failures,
		"processed": s.Processed,
		"working":   s.store.Working().Size(),
		"retries":   s.store.Retries().Size(),
		"scheduled": s.store.Scheduled().Size(),
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Result(bytes)
}

func processLines(conn *Connection, server *Server) {
	for {
		// every operation must complete within 1 second
		conn.conn.SetDeadline(time.Now().Add(1 * time.Second))

		cmd, e := conn.buf.ReadString('\n')
		if e != nil {
			conn.Close()
			return
		}
		cmd = strings.TrimSuffix(cmd, "\n")

		idx := strings.Index(cmd, " ")
		verb := cmd
		if idx >= 0 {
			verb = cmd[0:idx]
		}
		proc, ok := cmdSet[verb]
		if !ok {
			conn.Error(cmd, errors.New("unknown command"))
		} else {
			proc(conn, server, cmd)
		}
		if verb == "END" {
			break
		}
	}
}
