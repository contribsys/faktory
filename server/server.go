package server

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/storage"
	"github.com/mperham/faktory/util"
)

var (
	EventHandlers = make([]func(*Server), 0)
)

type ServerOptions struct {
	Binding     string
	StoragePath string
	Password    string
}

type RuntimeStats struct {
	Processed   int64
	Failures    int64
	Connections int64
	Commands    int64
	StartedAt   time.Time
}

type Server struct {
	Options *ServerOptions
	Stats   *RuntimeStats

	pwd        string
	listener   net.Listener
	store      storage.Store
	scheduler  *SchedulerSubsystem
	taskRunner *TaskRunner
	pending    *sync.WaitGroup
	mu         sync.Mutex
	heartbeats map[string]*ClientWorker
	hbmu       sync.Mutex
}

// register a global handler to be called when the Server instance
// has finished booting but before it starts listening.
func OnStart(x func(*Server)) {
	EventHandlers = append(EventHandlers, x)
}

func NewServer(opts *ServerOptions) *Server {
	if opts.Binding == "" {
		opts.Binding = "localhost:7419"
	}
	if opts.StoragePath == "" {
		opts.StoragePath = fmt.Sprintf("%s.db", strings.Replace(opts.Binding, ":", "_", -1))
	}
	return &Server{
		Options:    opts,
		Stats:      &RuntimeStats{StartedAt: time.Now()},
		pwd:        "123456",
		pending:    &sync.WaitGroup{},
		mu:         sync.Mutex{},
		heartbeats: make(map[string]*ClientWorker, 12),
		hbmu:       sync.Mutex{},
	}
}

func (s *Server) Heartbeats() map[string]*ClientWorker {
	return s.heartbeats
}

func (s *Server) Store() storage.Store {
	return s.store
}

func (s *Server) Start() error {
	store, err := storage.Open("rocksdb", s.Options.StoragePath)
	if err != nil {
		return err
	}
	defer store.Close()

	addr, err := net.ResolveTCPAddr("tcp", s.Options.Binding)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.store = store
	s.listener = listener
	s.StartScheduler(s.pending)
	s.StartTasks(s.pending)
	s.mu.Unlock()

	// wait for outstanding requests to finish
	defer s.pending.Wait()

	defer s.scheduler.Stop()
	defer s.taskRunner.Stop()

	for _, x := range EventHandlers {
		x(s)
	}

	// this is the central runtime loop for the main goroutine
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		go func() {
			s.pending.Add(1)
			defer s.pending.Done()

			s.processConnection(conn)
		}()
	}
}

func (s *Server) Stop(f func()) {
	// Don't allow new network connections
	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()
	time.Sleep(10 * time.Millisecond)

	if f != nil {
		f()
	}
}

func hash(pwd, salt string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(pwd+salt)))
}

func (s *Server) processConnection(conn net.Conn) {
	// AHOY operation must complete within 1 second
	conn.SetDeadline(time.Now().Add(1 * time.Second))

	buf := bufio.NewReader(conn)

	line, err := buf.ReadString('\n')
	if err != nil {
		util.Error("Closing connection", err, nil)
		conn.Close()
		return
	}

	valid := strings.HasPrefix(line, "AHOY {")
	if !valid {
		util.Info("Invalid preamble", line)
		util.Info("Need a valid AHOY")
		conn.Close()
		return
	}

	data := line[5:]
	var client ClientWorker
	err = json.Unmarshal([]byte(data), &client)
	if err != nil {
		util.Error("Invalid client data", err, nil)
		conn.Close()
		return
	}

	if s.Options.Password != "" &&
		subtle.ConstantTimeCompare([]byte(client.PasswordHash), []byte(hash(s.Options.Password, client.Salt))) != 1 {
		util.Info("Invalid password")
		conn.Close()
		return
	}

	if client.Wid == "" {
		util.Error("Invalid client Wid", err, nil)
		conn.Close()
		return
	}

	s.hbmu.Lock()
	val, ok := s.heartbeats[client.Wid]
	if ok {
		val.lastHeartbeat = time.Now()
	} else {
		client.StartedAt = time.Now()
		client.lastHeartbeat = time.Now()
		s.heartbeats[client.Wid] = &client
		val = &client
	}
	s.hbmu.Unlock()

	util.Debugf("%+v", val)

	_, err = conn.Write([]byte("+OK\r\n"))
	if err != nil {
		util.Error("Closing connection", err, nil)
		conn.Close()
		return
	}

	// disable deadline
	conn.SetDeadline(time.Time{})

	c := &Connection{
		client: &client,
		ident:  conn.RemoteAddr().String(),
		conn:   conn,
		buf:    buf,
	}

	processLines(c, s)
}

func processLines(conn *Connection, server *Server) {
	atomic.AddInt64(&server.Stats.Connections, 1)
	defer atomic.AddInt64(&server.Stats.Connections, -1)

	for {
		atomic.AddInt64(&server.Stats.Commands, 1)
		cmd, e := conn.buf.ReadString('\n')
		if e != nil {
			if e != io.EOF {
				util.Error("Unexpected socket error", e, nil)
			}
			conn.Close()
			return
		}
		cmd = strings.TrimSuffix(cmd, "\r\n")
		cmd = strings.TrimSuffix(cmd, "\n")
		//util.Debug(cmd)

		idx := strings.Index(cmd, " ")
		verb := cmd
		if idx >= 0 {
			verb = cmd[0:idx]
		}
		proc, ok := cmdSet[verb]
		if !ok {
			conn.Error(cmd, fmt.Errorf("Unknown command %s", verb))
		} else {
			proc(conn, server, cmd)
		}
		if verb == "END" {
			break
		}
	}
}

func parseJob(buf []byte) (*faktory.Job, error) {
	var job faktory.Job

	err := json.Unmarshal(buf, &job)
	if err != nil {
		return nil, err
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	return &job, nil
}
