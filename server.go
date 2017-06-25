package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
)

type Server struct {
	Binding    string
	pwd        string
	listener   net.Listener
	processors map[string]chan *Connection
	store      *storage.Store
}

func NewServer(binding string) *Server {
	if binding == "" {
		binding = "localhost:7419"
	}
	return &Server{Binding: binding, pwd: "123456", processors: make(map[string]chan *Connection)}
}

func (s *Server) Start() error {
	store, err := storage.OpenStore("")
	if err != nil {
		return err
	}
	s.store = store

	addr, err := net.ResolveTCPAddr("tcp", s.Binding)
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
			s.listener.Close()
			s.listener = nil
			return err
		}
		s.processConnection(conn)
	}

	return nil
}

func (s *Server) Stop(f func()) {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
	if f != nil {
		f()
	}
}

func (s *Server) processConnection(conn net.Conn) {
	buf := bufio.NewReader(conn)

	// The first line sent upon connection must be:
	//
	// AHOY pwd:<password> other:pair more:etc\n
	line, err := buf.ReadString('\n')
	if err != nil {
		fmt.Println("Closing connection: ", err)
		conn.Close()
		return
	}

	util.Debug(fmt.Sprintf("Cmd: %s", line))

	valid := strings.HasPrefix(line, "AHOY ")
	if !valid {
		fmt.Println("Invalid preamble", line)
		fmt.Println("Need a valid AHOY")
		conn.Close()
		return
	}

	pairs := strings.Split(line, " ")
	var attrs = make(map[string]string, len(pairs)-1)

	for _, pair := range pairs[1:] {
		two := strings.Split(pair, ":")
		if len(two) != 2 {
			fmt.Println("Invalid pair", pair)
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
				fmt.Println("Invalid password")
				conn.Close()
				return
			}
			attrs["pwd"] = "<redacted>"
		}
	}

	conn.Write([]byte("OK\n"))

	id, ok := attrs["id"]
	if !ok {
		id = conn.RemoteAddr().String()
	}
	c := &Connection{
		ident: id,
		conn:  conn,
		buf:   buf,
	}
	go process(c, s)
}

func process(conn *Connection, server *Server) {
	for {
		cmd, e := conn.buf.ReadString('\n')
		cmd = strings.TrimSuffix(cmd, "\n")
		if e != nil {
			util.Debug("Invalid socket input")
			conn.Close()
			break
		}
		//util.Debug(fmt.Sprintf("Cmd: %s", cmd))

		switch {
		case cmd == "END\n":
			conn.Ok()
			conn.Close()
			break
		case strings.HasPrefix(cmd, "POP "):
			qs := strings.Split(cmd, " ")[1:]
			job, err := Pop(func(job *Job) error {
				return server.Reserve(conn.Identity(), job)
			}, qs...)
			if err != nil {
				conn.Error(cmd, err)
				break
			}
			if job != nil {
				res, err := json.Marshal(job)
				if err != nil {
					conn.Error(cmd, err)
					break
				}
				conn.Result(res)
			} else {
				conn.Result([]byte("\n"))
			}
		case strings.HasPrefix(cmd, "PUSH {"):
			job, err := ParseJob([]byte(cmd[5:]))
			if err != nil {
				conn.Error(cmd, err)
				break
			}
			qname := job.Queue
			q := LookupQueue(qname)
			err = q.Push(job)
			if err != nil {
				conn.Error(cmd, err)
				break
			}
			conn.Ok()
		case strings.HasPrefix(cmd, "ACK "):
			jid := cmd[4:]
			err := server.Acknowledge(jid)
			if err != nil {
				conn.Error(cmd, err)
				break
			}

			conn.Ok()
		default:
			conn.Error(cmd, errors.New("unknown command"))
		}
	}
}

func (s *Server) Acknowledge(jid string) error {
	_, ok := workingMap[jid]
	if !ok {
		return fmt.Errorf("JID %s not found", jid)
	}
	delete(workingMap, jid)
	return nil
}

func (s *Server) ReapWorkingSet() (int, error) {
	now := time.Now()
	count := 0

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
	workingMap = map[string]*Reservation{}
)

func workingSize() int {
	return len(workingMap)
}

func (s *Server) Reserve(identity string, job *Job) error {
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
	workingMap[job.Jid] = res
	return nil
}
