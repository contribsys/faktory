package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
)

type Server struct {
	bind       string
	pwd        string
	listener   net.Listener
	processors map[string]chan *Connection
}

func NewServer() *Server {
	return &Server{bind: ":7419", pwd: "123456", processors: make(map[string]chan *Connection)}
}

func (s *Server) Start() error {
	addr, err := net.ResolveTCPAddr("tcp", s.bind)
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
			return err
		}
		s.processConnection(conn)
	}

	return nil
}

func (s *Server) Stop() {
	s.listener.Close()
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

	valid := strings.HasPrefix(line, "AHOY ")
	if !valid {
		fmt.Println("Invalid preamble", line)
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

	id, ok := attrs["id"]
	if !ok {
		id = conn.RemoteAddr().String()
	}
	app := "default"
	c, ok := s.processors[app]
	if ok == false {
		c = make(chan *Connection)
		s.processors[app] = c
		go process(c, app)
	}

	conn.Write([]byte("OK\n"))

	c <- &Connection{
		ident: id,
		conn:  conn,
		buf:   buf,
	}
}

func process(c chan *Connection, app string) {
	for {
		conn := <-c
		for {
			cmd, e := conn.buf.ReadString('\n')
			if e != nil {
				fmt.Println(e)
				conn.Close()
				break
			}

			fmt.Println(cmd)

			switch {
			case cmd == "END\n":
				conn.Ok()
				conn.Close()
				break
			case strings.HasPrefix(cmd, "POP "):
				qs := strings.Split(cmd, " ")[1:]
				job := conn.Pop(qs...)
				res, err := json.Marshal(job)
				if err != nil {
					conn.Error(err)
					break
				}
				conn.Result(res)
			case strings.HasPrefix(cmd, "PUSH {"):
				job, err := ParseJob([]byte(cmd[5:]))
				if err != nil {
					conn.Error(err)
					break
				}
				qname := job.Queue
				err = conn.Push(qname, job)
				if err != nil {
					conn.Error(err)
					break
				}

				conn.Result([]byte(job.Jid))
			default:
				conn.Error(errors.New("unknown command"))
			}
		}
	}
}
