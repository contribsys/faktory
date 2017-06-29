package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
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
	store     *storage.Store
}

func NewServer(opts *ServerOptions) *Server {
	if opts.Binding == "" {
		opts.Binding = "localhost:7419"
	}
	if opts.StoragePath == "" {
		opts.StoragePath = fmt.Sprintf("%s/%s.db",
			storage.DefaultPath, strings.Replace(opts.Binding, ":", "_", -1))
	}
	return &Server{Options: opts, pwd: "123456"}
}

func (s *Server) Start() error {
	store, err := storage.OpenStore(s.Options.StoragePath)
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
	s.listener.Close()

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

	util.DebugDebug(line)

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
	job, err := ParseJob([]byte(cmd[5:]))
	if err != nil {
		c.Error(cmd, err)
		return
	}
	qname := job.Queue
	q := LookupQueue(qname)
	err = q.Push(job)
	if err != nil {
		c.Error(cmd, err)
		return
	}
	c.Ok()
}

func pop(c *Connection, s *Server, cmd string) {
	qs := strings.Split(cmd, " ")[1:]
	job, err := Pop(func(job *Job) error {
		return Reserve(c.Identity(), job)
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

type JobFailure struct {
	Jid          string   `json:"jid"`
	RetryAt      string   `json:"retry_at"`
	ErrorMessage string   `json:"message"`
	ErrorType    string   `json:"errtype"`
	Backtrace    []string `json:"backtrace"`
}

func fail(c *Connection, s *Server, cmd string) {
	raw := cmd[5:]
	var failure JobFailure
	err := json.Unmarshal([]byte(raw), &failure)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	job, err := s.Acknowledge(failure.Jid)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	if job.Failure != nil {
		job.Failure.RetryCount += 1
		job.Failure.ErrorMessage = failure.ErrorMessage
		job.Failure.ErrorType = failure.ErrorType
		job.Failure.Backtrace = failure.Backtrace
	} else {
		job.Failure = &Failure{
			RetryCount:   0,
			FailedAt:     util.Nows(),
			ErrorMessage: failure.ErrorMessage,
			ErrorType:    failure.ErrorType,
			Backtrace:    failure.Backtrace,
		}
	}

	when := nextRetry(failure.RetryAt, job)
	bytes, err := json.Marshal(job)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	util.Info("Adding retry", job.Jid, when)
	s.store.Retries().AddElement(util.Thens(when), job.Jid, bytes)
	atomic.AddInt64(&s.Failures, 1)
	c.Ok()
}

var (
	// about one month
	maxRetryDelay = 720 * time.Hour
)

func nextRetry(override string, job *Job) time.Time {
	if override != "" {
		tm, err := time.Parse(time.RFC3339, override)
		if err != nil {
			util.Warn("Invalid retry_at: %s", override)
			return defaultRetry(job)
		}
		if tm.Before(time.Now()) || tm.After(time.Now().Add(maxRetryDelay)) {
			// retry time out of range
			util.Warn("Invalid retry_at time: %s", tm)
			return defaultRetry(job)
		}
		return tm
	}
	return defaultRetry(job)
}

func defaultRetry(job *Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1))
	return time.Now().Add(time.Duration(secs) * time.Second)
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
		//util.DebugDebug(cmd)

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

func (s *Server) Acknowledge(jid string) (*Job, error) {
	workingMutex.Lock()
	defer workingMutex.Unlock()

	res, ok := workingMap[jid]
	if !ok {
		return nil, fmt.Errorf("JID %s not found", jid)
	}
	delete(workingMap, jid)
	return res.Job, nil
}

func (s *Server) ReapWorkingSet() (int, error) {
	now := time.Now()
	count := 0

	workingMutex.Lock()
	defer workingMutex.Unlock()

	for jid, res := range workingMap {
		if res.texpiry.Before(now) {
			delete(workingMap, jid)
			count += 1
		}
	}

	return count, nil
}

type Reservation struct {
	Job     *Job   `json:"job"`
	Since   string `json:"reserved_at"`
	Expiry  string `json:"expires_at"`
	Who     string `json:"worker"`
	tsince  time.Time
	texpiry time.Time
}

var (
	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Bolt and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Bolt entry quickly.
	//
	// TODO Need to hydrate this map into memory when starting up
	// or a crash can leak reservations into the persistent Working
	// set.
	workingMap   = map[string]*Reservation{}
	workingMutex = &sync.Mutex{}
)

func workingSize() int {
	return len(workingMap)
}

func Reserve(identity string, job *Job) error {
	now := time.Now()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	exp := now.Add(time.Duration(timeout) * time.Second)
	var res = &Reservation{
		Job:     job,
		Since:   util.Thens(now),
		Expiry:  util.Thens(exp),
		Who:     identity,
		tsince:  now,
		texpiry: exp,
	}

	_, err := json.Marshal(res)
	if err != nil {
		return err
	}
	workingMutex.Lock()
	workingMap[job.Jid] = res
	workingMutex.Unlock()
	return nil
}
