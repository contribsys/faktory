package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
)

type Options struct {
	Pwd    string
	Format string
}

func (opt *Options) String() string {
	return fmt.Sprintf("Pwd:%s Format:%s", opt.Pwd, opt.Format)
}

type Client struct {
	Hostname string
	Port     int
	Options  *Options
	rdr      *bufio.Reader
	wtr      *bufio.Writer
	conn     net.Conn
}

/*
 * Open a connection to the remote Worq server.
 * You must include a 'pwd' parameter if the server is configured to require
 * a password:
 *
 *   worq.Dial("localhost", 7419, Parameters{"pwd":"topsecret", "another":"thing"})
 *
 */
func Dial(host string, port int, params *Options) (*Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	if params == nil {
		params = &Options{}
	}
	if params.Format == "" {
		params.Format = "json"
	}

	_, err = w.WriteString("AHOY ")
	_, err = w.WriteString(params.String())
	_, err = w.WriteString("\n")
	err = w.Flush()
	if err != nil {
		conn.Close()
		return nil, err
	}

	line, err := r.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, err
	}
	if line != "OK\n" {
		conn.Close()
		return nil, errors.New(line)
	}

	return &Client{Hostname: host, Port: port, Options: params, conn: conn, rdr: r, wtr: w}, nil
}

func (c *Client) Close() error {
	_, err := c.wtr.Write([]byte("END\n"))
	err = c.wtr.Flush()
	return err
}

func (c *Client) Ack(jid string) error {
	_, err := c.wtr.WriteString("ACK ")
	_, err = c.wtr.WriteString(jid)
	_, err = c.wtr.WriteString("\n")
	err = c.wtr.Flush()
	if err != nil {
		return err
	}
	line, err := c.rdr.ReadString('\n')
	if err != nil {
		return err
	}
	if line == "OK\n" {
		// normal return
		return nil
	}
	if strings.HasPrefix(line, "ERR ") {
		return errors.New(line[4 : len(line)-1])
	}
	return errors.New(line)
}

func (c *Client) Push(job *Job) error {
	jobytes, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = c.wtr.WriteString("PUSH ")
	_, err = c.wtr.Write(jobytes)
	_, err = c.wtr.WriteString("\n")
	err = c.wtr.Flush()
	if err != nil {
		return err
	}
	line, err := c.rdr.ReadString('\n')
	if err != nil {
		return err
	}
	if line == "OK\n" {
		// normal return
		return nil
	}
	if strings.HasPrefix(line, "ERR ") {
		return errors.New(line[4 : len(line)-1])
	}
	return errors.New(line)
}

func (c *Client) Pop(q string) (*Job, error) {
	_, err := c.wtr.WriteString(fmt.Sprintf("POP %s\n", q))
	err = c.wtr.Flush()
	if err != nil {
		return nil, err
	}
	line, err := c.rdr.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(line, "ERR ") {
		return nil, errors.New(line[4 : len(line)-1])
	}

	var job Job
	err = json.Unmarshal([]byte(line), &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}
