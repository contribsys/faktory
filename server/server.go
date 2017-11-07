package server

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

var (
	EventHandlers = make([]func(*Server) error, 0)
)

type ServerOptions struct {
	Binding          string
	StorageDirectory string
	ConfigDirectory  string
	Environment      string
	DisableTls       bool
}

type RuntimeStats struct {
	Processed   int64
	Failures    int64
	Connections int64
	Commands    int64
	StartedAt   time.Time
}

type Server struct {
	Options   *ServerOptions
	Stats     *RuntimeStats
	TLSConfig *tls.Config
	Password  string

	listener   net.Listener
	store      storage.Store
	taskRunner *taskRunner
	pending    *sync.WaitGroup
	mu         sync.Mutex
	heartbeats map[string]*ClientWorker
	hbmu       sync.RWMutex

	initialized chan bool
}

// register a global handler to be called when the Server instance
// has finished booting but before it starts listening.
func OnStart(x func(*Server) error) {
	EventHandlers = append(EventHandlers, x)
}

func NewServer(opts *ServerOptions) (*Server, error) {
	if opts.Binding == "" {
		opts.Binding = "localhost:7419"
	}
	if opts.StorageDirectory == "" {
		return nil, fmt.Errorf("empty storage directory")
	}

	s := &Server{
		Options:     opts,
		Stats:       &RuntimeStats{StartedAt: time.Now()},
		pending:     &sync.WaitGroup{},
		heartbeats:  make(map[string]*ClientWorker, 12),
		initialized: make(chan bool, 1),
	}

	tlsC, err := tlsConfig(s.Options.Binding, s.Options.DisableTls, s.Options.ConfigDirectory)
	if err != nil {
		return nil, err
	}
	if tlsC != nil {
		s.TLSConfig = tlsC
		pwd, err := fetchPassword(s.Options.ConfigDirectory)
		if err != nil {
			return nil, err
		}
		s.Password = pwd

		// if we need TLS, we need a password too
		if s.Password == "" {
			return nil, fmt.Errorf("Cannot enable TLS without a password")
		}
	}

	return s, nil
}

func (s *Server) Heartbeats() map[string]*ClientWorker {
	return s.heartbeats
}

func (s *Server) Store() storage.Store {
	return s.store
}

func (s *Server) Start() error {
	store, err := storage.Open("rocksdb", s.Options.StorageDirectory)
	if err != nil {
		return err
	}
	defer store.Close()

	var listener net.Listener

	if s.TLSConfig != nil {
		listener, err = tls.Listen("tcp", s.Options.Binding, s.TLSConfig)
		if err != nil {
			return err
		}
		util.Infof("Now listening securely at %s, press Ctrl-C to stop", s.Options.Binding)
	} else {
		listener, err = net.Listen("tcp", s.Options.Binding)
		if err != nil {
			return err
		}
		util.Infof("Now listening at %s, press Ctrl-C to stop", s.Options.Binding)
	}

	s.mu.Lock()
	s.store = store
	s.listener = listener
	s.loadWorkingSet()
	s.startTasks(s.pending)
	s.startScanners(s.pending)
	s.mu.Unlock()

	s.initialized <- true

	// wait for outstanding requests to finish
	defer s.pending.Wait()
	defer s.taskRunner.Stop()

	for _, x := range EventHandlers {
		err := x(s)
		if err != nil {
			return err
		}
	}

	// this is the central runtime loop for the main goroutine
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		s.pending.Add(1)
		go func(conn net.Conn) {
			defer s.pending.Done()

			c := startConnection(conn, s)
			if c == nil {
				return
			}
			processLines(c, s)
		}(conn)
	}
}

func (s *Server) WaitUntilInitialized() {
	<-s.initialized
}

func (s *Server) Stop(f func()) {
	// Don't allow new network connections
	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()
	time.Sleep(100 * time.Millisecond)

	if f != nil {
		f()
	}
}

func hash(pwd, salt string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(pwd+salt)))
}

var (
	ProtocolVersion = []byte(`"1"`)
)

func startConnection(conn net.Conn, s *Server) *Connection {
	// handshake must complete within 1 second
	conn.SetDeadline(time.Now().Add(1 * time.Second))

	var salt string
	conn.Write([]byte(`+HI {"v":`))
	conn.Write(ProtocolVersion)
	if s.Password != "" {
		salt = strconv.FormatInt(rand.Int63(), 16)
		conn.Write([]byte(`,"s":"`))
		conn.Write([]byte(salt))
		conn.Write([]byte(`"}`))
	} else {
		conn.Write([]byte("}"))
	}
	conn.Write([]byte("\r\n"))

	buf := bufio.NewReader(conn)

	line, err := buf.ReadString('\n')
	if err != nil {
		util.Error("Closing connection", err)
		conn.Close()
		return nil
	}

	valid := strings.HasPrefix(line, "HELLO {")
	if !valid {
		util.Infof("Invalid preamble: %s", line)
		util.Info("Need a valid HELLO")
		conn.Close()
		return nil
	}

	client, err := clientWorkerFromHello(line[5:])
	if err != nil {
		util.Error("Invalid client data in HELLO", err)
		conn.Close()
		return nil
	}

	if s.Password != "" {
		if subtle.ConstantTimeCompare([]byte(client.PasswordHash), []byte(hash(s.Password, salt))) != 1 {
			conn.Write([]byte("-ERR Invalid password\r\n"))
			conn.Close()
			return nil
		}
	}

	if client.Wid == "" {
		// a producer, not a consumer connection
	} else {
		updateHeartbeat(client, s.heartbeats, &s.hbmu)
	}

	_, err = conn.Write([]byte("+OK\r\n"))
	if err != nil {
		util.Error("Closing connection", err)
		conn.Close()
		return nil
	}

	// disable deadline
	conn.SetDeadline(time.Time{})

	return &Connection{
		client: client,
		conn:   conn,
		buf:    buf,
	}
}

func processLines(conn *Connection, server *Server) {
	atomic.AddInt64(&server.Stats.Connections, 1)
	defer atomic.AddInt64(&server.Stats.Connections, -1)

	for {
		atomic.AddInt64(&server.Stats.Commands, 1)
		cmd, e := conn.buf.ReadString('\n')
		if e != nil {
			if e != io.EOF {
				util.Error("Unexpected socket error", e)
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
