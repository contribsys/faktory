package server

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

type RuntimeStats struct {
	Connections uint64
	Commands    uint64
	StartedAt   time.Time
}

type Server struct {
	Options    *ServerOptions
	Stats      *RuntimeStats
	Subsystems []Subsystem

	TLSPublicCert string
	TLSPrivateKey string

	tlsConfig  *tls.Config
	listener   net.Listener
	store      storage.Store
	manager    manager.Manager
	workers    *workers
	taskRunner *taskRunner
	mu         sync.Mutex
	stopper    chan bool
	closed     bool
}

func (s *Server) useTLS() error {
	privateKey := filepath.Join(s.Options.ConfigDirectory, "private.key.pem")
	publicCert := filepath.Join(s.Options.ConfigDirectory, "public.cert.pem")

	if _, err := os.Stat(privateKey); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if _, err := os.Stat(publicCert); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	x, err := os.Open(privateKey)
	if err != nil {
		return fmt.Errorf("Unable to open private key: %w", err)
	}
	defer x.Close()
	y, err := os.Open(publicCert)
	if err != nil {
		return fmt.Errorf("Unable to open public cert: %w", err)
	}
	defer y.Close()
	cert, err := tls.LoadX509KeyPair(publicCert, privateKey)
	if err != nil {
		return fmt.Errorf("Unable to initialize TLS certificate: %w", err)
	}
	util.Infof("TLS activated with %s", publicCert)

	s.TLSPublicCert = publicCert
	s.TLSPrivateKey = privateKey
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	return nil
}

func NewServer(opts *ServerOptions) (*Server, error) {
	if opts.Binding == "" {
		opts.Binding = "localhost:7419"
	}
	if opts.StorageDirectory == "" {
		return nil, fmt.Errorf("missing or empty storage directory")
	}

	s := &Server{
		Options:    opts,
		Stats:      &RuntimeStats{StartedAt: time.Now()},
		Subsystems: []Subsystem{},

		stopper: make(chan bool),
		closed:  false,
	}

	return s, nil
}

func (s *Server) Heartbeats() map[string]*ClientData {
	return s.workers.heartbeats
}

func (s *Server) Store() storage.Store {
	return s.store
}

func (s *Server) Manager() manager.Manager {
	return s.manager
}

func (s *Server) Reload() {
	for idx := range s.Subsystems {
		subsystem := s.Subsystems[idx]
		if err := subsystem.Reload(s); err != nil {
			util.Warnf("Subsystem %s returned reload error: %v", subsystem.Name(), err)
		}
	}
}

func (s *Server) AddTask(everySec int64, task Taskable) {
	s.taskRunner.AddTask(everySec, task)
}

func (s *Server) Boot() error {
	store, err := storage.Open(s.Options.RedisSock, s.Options.PoolSize)
	if err != nil {
		return fmt.Errorf("cannot open redis database: %w", err)
	}

	err = s.useTLS()
	if err != nil {
		return err
	}

	var listener net.Listener
	if s.tlsConfig != nil {
		listener, err = tls.Listen("tcp", s.Options.Binding, s.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", s.Options.Binding)
	}
	if err != nil {
		store.Close()
		return fmt.Errorf("cannot listen on %s: %w", s.Options.Binding, err)
	}

	s.mu.Lock()
	s.store = store
	s.workers = newWorkers()
	s.manager = manager.NewManager(store)
	s.listener = listener
	s.stopper = make(chan bool)
	s.startTasks()
	s.mu.Unlock()

	return nil
}

func (s *Server) Run() error {
	if s.store == nil {
		panic("Server hasn't been booted")
	}

	for idx := range s.Subsystems {
		subsystem := s.Subsystems[idx]
		if err := subsystem.Start(s); err != nil {
			close(s.Stopper())
			return fmt.Errorf("cannot start server subsystem %s: %w", subsystem.Name(), err)
		}
	}

	util.Infof("PID %d listening at %s, press Ctrl-C to stop", os.Getpid(), s.Options.Binding)

	// this is the runtime loop for the command server
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		// Each connection gets its own goroutine which ultimately limits Faktory's scalability.
		// Faktory hardcodes a limit of `DefaultMaxPoolSize` Redis connections but does not put a limit here
		// because Go's runtime scheduler will get better over time.
		// TODO: Look into alternatives like a reactor + goroutine pool.
		go func(conn net.Conn) {
			c := startConnection(conn, s)
			if c == nil {
				return
			}
			defer cleanupConnection(s, c)
			// util.Debugf("Creating client connection %+v %s", c, c.client.Wid)
			s.processLines(c)
		}(conn)
	}
}

func (s *Server) Stopper() chan bool {
	return s.stopper
}

func (s *Server) Stop(f func()) {
	// Don't allow new network connections
	s.mu.Lock()
	s.closed = true
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	if f != nil {
		f()
	}

	s.store.Close()
}

func cleanupConnection(s *Server, c *Connection) {
	// util.Debugf("Removing client connection %+v", c)
	s.workers.RemoveConnection(c)
}

func hash(pwd, salt string, iterations int) string {
	bytes := []byte(pwd + salt)
	hash := sha256.Sum256(bytes)
	if iterations > 1 {
		for i := 1; i < iterations; i++ {
			hash = sha256.Sum256(hash[:])
		}
	}
	return fmt.Sprintf("%x", hash)
}

func startConnection(conn net.Conn, s *Server) *Connection {
	// Handshake must complete within 2 seconds.
	// This is a DoS mitigation so clients can't start a handshake
	// but never complete it, leaving a connection open.
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// 4000 iterations is about 1ms on my 2016 MBP w/ 2.9Ghz Core i5
	iter := rand.Intn(4096) + 4000 //nolint:gosec

	saltValue, err := util.RandomInt63()
	if err != nil {
		util.Error("Couldn't produce salt.", err)
	}

	var salt string
	_, _ = conn.Write([]byte(`+HI {"v":2`))
	if s.Options.Password != "" {
		_, _ = conn.Write([]byte(`,"i":`))
		iters := strconv.FormatInt(int64(iter), 10)
		_, _ = conn.Write([]byte(iters))
		salt = strconv.FormatInt(saltValue, 16)
		_, _ = conn.Write([]byte(`,"s":"`))
		_, _ = conn.Write([]byte(salt))
		_, _ = conn.Write([]byte(`"}`))
	} else {
		_, _ = conn.Write([]byte("}"))
	}
	_, _ = conn.Write([]byte("\r\n"))

	buf := bufio.NewReader(conn)

	line, err := buf.ReadString('\n')
	if err != nil {
		defer conn.Close()
		// TCP probes on the socket will close connection
		// immediately and lead to EOF. Don't flood logs with them.
		if errors.Is(err, io.EOF) {
			return nil
		}

		opErr, ok := err.(net.Error)
		if ok && opErr.Timeout() {
			// util.Debugf("Error establishing connection with client %v", err)
			return nil
		}
		_, ok = err.(tls.RecordHeaderError)
		if ok {
			// util.Debugf("Client not using TLS %v", err)
			return nil

		}
		util.Infof("Bad connection %T: %v", err, err)
		return nil
	}

	valid := strings.HasPrefix(line, "HELLO {")
	if !valid {
		util.Debugf("Invalid preamble: %s", line)
		util.Debug("Need a valid HELLO, is client using TLS?")
		conn.Close()
		return nil
	}

	cl, err := clientDataFromHello(line[5:])
	if err != nil {
		util.Error("Invalid client data in HELLO", err)
		conn.Close()
		return nil
	}

	if s.Options.Password != "" {
		if cl.Version < 2 {
			iter = 1
		}

		if subtle.ConstantTimeCompare([]byte(cl.PasswordHash), []byte(hash(s.Options.Password, salt, iter))) != 1 {
			_, _ = conn.Write([]byte("-ERR Invalid password\r\n"))
			_ = conn.Close()
			return nil
		}
	}

	cn := &Connection{
		client: cl,
		conn:   conn,
		buf:    buf,
	}

	if cl.Wid == "" {
		// a producer, not a consumer connection
	} else {
		s.workers.setupHeartbeat(cl, cn)
	}

	_, err = conn.Write([]byte("+OK\r\n"))
	if err != nil {
		util.Error("Closing connection", err)
		conn.Close()
		return nil
	}

	// disable deadline
	_ = conn.SetDeadline(time.Time{})

	// if cl.Username != "" {
	// util.Debugf("Successful connection from %s", cl.Username)
	// }

	return cn
}

func (s *Server) processLines(conn *Connection) {
	atomic.AddUint64(&s.Stats.Connections, 1)
	defer atomic.AddUint64(&s.Stats.Connections, ^uint64(0))
	defer conn.Close()

	if s.Stats.Connections > uint64(s.Options.PoolSize) {
		if client.Name == "Faktory" {
			// This will trigger in Faktory OSS if over the default max pool size.
			util.Warnf("%s has over %d active client connections and may exhibit poor performance. Ensure your worker processes are using a connection pool and closing unused connections.", client.Name, s.Options.PoolSize)
		} else {
			// This will trigger in Faktory Enterprise if over the licensed connection count.
			util.Warnf("%s has over %d active client connections and may exhibit poor performance. Ensure your worker processes are using no more than your licensed connection count.", client.Name, s.Options.PoolSize)
		}
	}

	for {
		cmd, e := conn.buf.ReadString('\n')
		if e != nil {
			if e != io.EOF {
				util.Error("Unexpected socket error", e)
			}
			return
		}
		if s.closed {
			_ = conn.Error("Closing connection", fmt.Errorf("shutdown in progress"))
			return
		}
		cmd = strings.TrimSuffix(cmd, "\r\n")
		cmd = strings.TrimSuffix(cmd, "\n")
		// util.Debug(cmd)

		idx := strings.Index(cmd, " ")
		verb := cmd
		if idx >= 0 {
			verb = cmd[0:idx]
		}
		proc, ok := CommandSet[verb]
		if !ok {
			_ = conn.Error(cmd, fmt.Errorf("unknown command %s", verb))
		} else {
			atomic.AddUint64(&s.Stats.Commands, 1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5*time.Second))
			conn.Context = ctx
			proc(conn, s, cmd)
			cancel()
		}
		if verb == "END" {
			break
		}
	}
}

func (s *Server) uptimeInSeconds() int {
	return int(time.Since(s.Stats.StartedAt).Seconds())
}

func (s *Server) CurrentState() (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	queueCmd := map[string]*redis.IntCmd{}
	_, err := s.store.Redis().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		s.store.EachQueue(ctx, func(q storage.Queue) {
			queueCmd[q.Name()] = pipe.LLen(ctx, q.Name())
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	queues := map[string]int64{}
	totalQueued := int64(0)
	totalQueues := len(queueCmd)
	for name, cmd := range queueCmd {
		qsize := cmd.Val()
		totalQueued += qsize
		queues[name] = qsize
	}

	return map[string]interface{}{
		"now":             util.Nows(),
		"server_utc_time": time.Now().UTC().Format("15:04:05 UTC"),
		"faktory": map[string]interface{}{
			"total_failures":  s.store.TotalFailures(ctx),
			"total_processed": s.store.TotalProcessed(ctx),
			"total_enqueued":  totalQueued,
			"total_queues":    totalQueues,
			"queues":          queues,
			"tasks":           s.taskRunner.Stats(),
		},
		"server": map[string]interface{}{
			"description":     client.Name,
			"faktory_version": client.Version,
			"uptime":          s.uptimeInSeconds(),
			"connections":     atomic.LoadUint64(&s.Stats.Connections),
			"command_count":   atomic.LoadUint64(&s.Stats.Commands),
			"used_memory_mb":  util.MemoryUsageMB(),
		},
	}, nil
}
