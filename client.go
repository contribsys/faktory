package worq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
)

type ClientOptions struct {
	Hostname string
	Port     int
	Pwd      string
	Format   string
}

func (opt *ClientOptions) String() string {
	return fmt.Sprintf("Pwd:%s Format:%s", opt.Pwd, opt.Format)
}

type Client struct {
	Options *ClientOptions
	rdr     *bufio.Reader
	wtr     *bufio.Writer
	conn    net.Conn
}

/*
 * Open a connection to the remote Worq server.
 * You must include a 'pwd' parameter if the server is configured to require
 * a password:
 *
 *   worq.Dial(&worq.ClientOptions{
 *												 Pwd: "topsecret",
 *												 Hostname: "localhost",
 *		  									 Port: 7419})
 *
 */
func Dial(params *ClientOptions) (*Client, error) {
	if params == nil {
		params = &ClientOptions{}
	}
	if params.Format == "" {
		params.Format = "json"
	}
	if params.Hostname == "" {
		params.Hostname = "localhost"
	}
	if params.Port == 0 {
		params.Port = 7419
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", params.Hostname, params.Port))
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	err = writeLine(w, "AHOY", []byte(params.String()))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ok(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{Options: params, conn: conn, rdr: r, wtr: w}, nil
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
		"jid":     jid,
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
	err = writeLine(c.wtr, "FAIL", failbytes)
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

//////////////////////////////////////////////////

func writeLine(io *bufio.Writer, op string, payload []byte) error {
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
		_, err = io.Write([]byte("\n"))
	}
	if err == nil {
		err = io.Flush()
	}
	return err
}

func ok(io *bufio.Reader) error {
	line, err := io.ReadString('\n')
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

func jsonResult(io *bufio.Reader, thing interface{}) error {
	line, err := io.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.HasPrefix(line, "ERR ") {
		return errors.New(line[4 : len(line)-1])
	}

	err = json.Unmarshal([]byte(line), thing)
	if err != nil {
		return err
	}
	return nil
}
