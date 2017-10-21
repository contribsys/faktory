package faktory

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Location string
	Options  *ClientData
	rdr      *bufio.Reader
	wtr      *bufio.Writer
	conn     net.Conn
}

/*
 * This data is serialized to JSON and sent
 * with the AHOY command.  PasswordHash is required
 * if the server is not listening on localhost.
 * The WID (worker id) must be random and unique
 * for each worker process.  It can be a UUID, etc.
 *
 * The other elements can be useful for debugging
 * and are displayed on the Busy tab.
 */
type ClientData struct {
	Hostname string   `json:"hostname"`
	Wid      string   `json:"wid"`
	Pid      int      `json:"pid"`
	Labels   []string `json:"labels"`
	// Salt should be a random string and
	// must change on every call.
	Salt string `json:"salt"`
	// Hash is hex(sha256(password + salt))
	PasswordHash string `json:"pwdhash"`
}

type Server struct {
	Network string
	Address string
	Timeout time.Duration
}

var (
	RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)
)

func DefaultServer() *Server {
	return &Server{"tcp", "localhost:7419", 1 * time.Second}
}

/*
 * This function connects to a Faktory server based on the
 * environment variable conventions:
 *
 * - Use FAKTORY_PROVIDER to point to a custom URL variable.
 * - Use FAKTORY_URL as a catch-all default.
 */
func Open() (*Client, error) {
	srv := DefaultServer()

	val, ok := os.LookupEnv("FAKTORY_PROVIDER")
	if ok {
		if strings.Contains(val, ":") {
			return nil, fmt.Errorf(`Error: FAKTORY_PROVIDER is not a URL. It is the name of the ENV var that contains the URL:

FAKTORY_PROVIDER=FOO_URL
FOO_URL=tcp://:mypassword@faktory.example.com:7419`)
		}

		uval, ok := os.LookupEnv(val)
		if ok {
			uri, err := url.Parse(uval)
			if err != nil {
				return nil, err
			}
			srv.Network = uri.Scheme
			srv.Address = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
			pwd := ""
			if uri.User != nil {
				pwd, _ = uri.User.Password()
			}
			return Dial(srv, pwd)
		}
		return nil, fmt.Errorf("FAKTORY_PROVIDER set to invalid value: %s", val)
	}

	uval, ok := os.LookupEnv("FAKTORY_URL")
	if ok {
		uri, err := url.Parse(uval)
		if err != nil {
			return nil, err
		}
		srv.Network = uri.Scheme
		srv.Address = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
		pwd := ""
		if uri.User != nil {
			pwd, _ = uri.User.Password()
		}
		return Dial(srv, pwd)
	}

	// Connect to default localhost
	return Dial(srv, "")
}

/*
 * Open a connection to the remote faktory server.
 *
 *   faktory.Dial(faktory.Localhost, "topsecret")
 *
 */
func Dial(srv *Server, password string) (*Client, error) {
	client := emptyClientData()

	local, err := regexp.Match("\\Alocalhost:", []byte(srv.Address))
	if err != nil {
		return nil, err
	}
	//util.Debugf("Connecting to %v TLS:%v", srv, !local)

	var conn net.Conn
	dial := &net.Dialer{Timeout: srv.Timeout}
	if local {
		conn, err = dial.Dial(srv.Network, srv.Address)
		if err != nil {
			return nil, err
		}
	} else {
		conn, err = tls.DialWithDialer(dial, srv.Network, srv.Address, &tls.Config{})
		if err != nil {
			return nil, err
		}
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	line, err := readString(r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if strings.HasPrefix(line, "HI ") {
		str := strings.TrimSpace(line)[3:]

		var hi map[string]string
		err = json.Unmarshal([]byte(str), &hi)
		if err != nil {
			conn.Close()
			return nil, err
		}
		salt, ok := hi["s"]
		if ok {
			client.PasswordHash = fmt.Sprintf("%x", sha256.Sum256([]byte(password+salt)))
		}
	} else {
		conn.Close()
		return nil, fmt.Errorf("Expecting HI but got: %s", line)
	}

	data, err := json.Marshal(client)
	if err != nil {
		return nil, err
	}

	err = writeLine(w, "HELLO", data)
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ok(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{Options: client, Location: srv.Address, conn: conn, rdr: r, wtr: w}, nil
}

func (c *Client) Close() error {
	return writeLine(c.wtr, "END", nil)
}

func (c *Client) Ack(jid string) error {
	err := writeLine(c.wtr, "ACK", []byte(fmt.Sprintf(`{"jid":"%s"}`, jid)))
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

func (c *Client) Fetch(q ...string) (*Job, error) {
	err := writeLine(c.wtr, "FETCH", []byte(strings.Join(q, " ")))
	if err != nil {
		return nil, err
	}

	data, err := readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	if data == nil || len(data) == 0 {
		return nil, nil
	}

	var job Job
	err = json.Unmarshal(data, &job)
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

/*
 * Notify Faktory that a job failed with the given error.
 * If backtrace is non-nil, it is assumed to be the output from
 * runtime/debug.Stack().
 */
func (c *Client) Fail(jid string, err error, backtrace []byte) error {
	failure := map[string]interface{}{
		"message": err.Error(),
		"errtype": "unknown",
		"jid":     jid,
	}

	if backtrace != nil {
		str := string(backtrace)
		bt := strings.Split(str, "\n")
		failure["backtrace"] = bt[3:]
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

func (c *Client) Flush() error {
	err := writeLine(c.wtr, "FLUSH", nil)
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

	data, err := readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	if data == nil || len(data) == 0 {
		return nil, nil
	}

	var hash map[string]interface{}
	err = json.Unmarshal(data, &hash)
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

func (c *Client) Beat() (string, error) {
	val, err := c.Generic("BEAT " + fmt.Sprintf(`{"wid":"%s"}`, RandomProcessWid))
	if val == "OK" {
		return "", nil
	}
	return val, err
}

//////////////////////////////////////////////////

func emptyClientData() *ClientData {
	client := &ClientData{}
	hs, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	client.Hostname = hs
	client.Pid = os.Getpid()
	client.Wid = RandomProcessWid
	client.Labels = []string{"golang"}
	return client
}

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
