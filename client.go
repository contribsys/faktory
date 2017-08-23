package faktory

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
)

type Client struct {
	Options *ClientData
	rdr     *bufio.Reader
	wtr     *bufio.Writer
	conn    net.Conn
}

type ClientData struct {
	Hostname    string
	Wid         string
	Pid         int
	Concurrency int
	Busy        int
	Labels      []string
	AppName     string
	Password    string
}

var (
	RandomProcessWid = strconv.FormatInt(rand.Int63(), 10)
)

func EmptyClientData() *ClientData {
	client := &ClientData{}
	hs, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	client.Hostname = hs
	client.Pid = os.Getpid()
	client.Wid = RandomProcessWid
	client.Concurrency = 1
	client.Labels = []string{}
	return client
}

/*
 * Open a connection to the remote faktory server.
 * You must include a 'pwd' parameter if the server is configured to require
 * a password:
 *
 *   faktory.Dial("localhost:7419", "topsecret")
 *
 */
func Dial(location string, password string) (*Client, error) {
	client := EmptyClientData()
	client.Password = password
	data, err := json.Marshal(client)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", location)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	err = writeLine(w, "AHOY", data)
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ok(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{Options: client, conn: conn, rdr: r, wtr: w}, nil
}

func (c *Client) Close() error {
	return writeLine(c.wtr, "END", nil)
}

func (c *Client) Ack(jid string) error {
	err := writeLine(c.wtr, "ACK", []byte(jid))
	if err != nil {
		return err
	}

	return ok(c.rdr)
}

func (c *Client) Push(job *Job) error {
	jobytes, err := json.Marshal(job)
	if err != nil {
		return err
	}
	err = writeLine(c.wtr, "PUSH", jobytes)
	if err != nil {
		return err
	}
	return ok(c.rdr)
}

func (c *Client) Pop(q string) (*Job, error) {
	err := writeLine(c.wtr, "POP", []byte(q))
	if err != nil {
		return nil, err
	}

	var job Job
	err = jsonResult(c.rdr, &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

/*
 buff := make([]byte, 4096)
 count := runtime.Stack(buff, false)
 str := string(buff[0:count])
 bt := strings.Split(str, "\n")
*/

func (c *Client) Fail(jid string, err error, backtrace []string) error {
	failure := map[string]interface{}{
		"message": err.Error(),
		"errtype": reflect.TypeOf(err).Name(),
	}

	if backtrace != nil {
		failure["backtrace"] = backtrace
	}
	failbytes, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	err = writeLine(c.wtr, "FAIL "+jid, failbytes)
	if err != nil {
		return err
	}
	return ok(c.rdr)
}

func (c *Client) Info() (map[string]interface{}, error) {
	err := writeLine(c.wtr, "INFO", nil)
	if err != nil {
		return nil, err
	}

	var hash map[string]interface{}

	err = jsonResult(c.rdr, &hash)
	if err != nil {
		return nil, err
	}

	return hash, nil
}

func (c *Client) Generic(cmdline string) (string, error) {
	err := writeLine(c.wtr, cmdline, nil)
	if err != nil {
		return "", err
	}

	return readString(c.rdr)
}

//////////////////////////////////////////////////

func writeLine(io *bufio.Writer, op string, payload []byte) error {
	//util.Debugf("> %s %s", op, string(payload))

	_, err := io.Write([]byte(op))
	if payload != nil {
		if err == nil {
			_, err = io.Write([]byte(" "))
		}
		if err == nil {
			_, err = io.Write(payload)
		}
	}
	if err == nil {
		_, err = io.Write([]byte("\r\n"))
	}
	if err == nil {
		err = io.Flush()
	}
	return err
}

func ok(rdr *bufio.Reader) error {
	val, err := readResponse(rdr)
	if err != nil {
		return err
	}
	if string(val) == "OK" {
		return nil
	}

	return fmt.Errorf("Invalid response: %s", string(val))
}

func readString(rdr *bufio.Reader) (string, error) {
	val, err := readResponse(rdr)
	if err != nil {
		return "", err
	}

	return string(val), nil
}

type ProtocolError struct {
	msg string
}

func (pe *ProtocolError) Error() string {
	return pe.msg
}

func readResponse(rdr *bufio.Reader) ([]byte, error) {
	chr, err := rdr.ReadByte()
	if err != nil {
		return nil, err
	}

	line, err := rdr.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]

	switch chr {
	case '$':
		// read length $10\r\n
		count, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		var buff []byte
		if count > 0 {
			buff = make([]byte, count)
			_, err = io.ReadFull(rdr, buff)
			if err != nil {
				return nil, err
			}
		}
		_, err = rdr.ReadString('\n')
		if err != nil {
			return nil, err
		}
		//util.Debugf("< %s%s", string(chr), string(line))
		//util.Debugf("< %s", string(buff))
		return buff, nil
	case '-':
		return nil, &ProtocolError{msg: string(line)}
	default:
		//util.Debugf("< %s%s", string(chr), string(line))
		return line, nil
	}
}

func jsonResult(rdr *bufio.Reader, thing interface{}) error {
	data, err := readResponse(rdr)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, thing)
	if err != nil {
		return err
	}
	return nil
}
