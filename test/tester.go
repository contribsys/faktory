package tester

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
	"github.com/mperham/worq/util"
)

func TestSystem() {
	cli.SetupLogging(os.Stdout)
	opts := cli.ParseArguments()
	s := worq.NewServer(opts.Binding)

	go cli.HandleSignals(s)
	go pushAndPop()

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func pushAndPop() {
	conn, err := net.Dial("tcp", "localhost:7419")
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 10000; i++ {
		if err = pushJob(conn, i); err != nil {
			fmt.Println(err)
			return
		}
	}

	for i := 0; i < 10000; i++ {
		job, err := pop(conn)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = ack(conn, job)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func pushJob(conn net.Conn, idx int) error {
	j := &worq.Job{
		Jid:   util.RandomJid(),
		Queue: "default",
		Class: "SomeJob",
		Args:  []interface{}{1, "string", 3},
	}
	jobytes, err := json.Marshal(j)
	if err != nil {
		return err
	}
	conn.Write([]byte("PUSH "))
	conn.Write(jobytes)
	conn.Write([]byte("\n"))
	return nil
}

func pop(conn net.Conn) (*worq.Job, error) {
	conn.Write("POP default\n")
	buf := bufio.NewReader(conn)
	line, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}

	var job worq.Job
	err = json.Unmarshal(line, &job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func ack(conn net.Conn, job *worq.Job) error {
	conn.Write([]byte("ACK "))
	conn.Write([]byte(job.Jid))
	conn.Write([]byte("\n"))
	buf := bufio.NewReader(conn)
	line, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if line != "OK\n" {
		return nil, errors.New("Bad acknowledgment: " + line)
	}
	return nil
}
