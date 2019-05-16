package client

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/contribsys/faktory/util"
)

const (
	// This is the protocol version supported by this client.
	// The server might be running an older or newer version.
	ExpectedProtocolVersion = 2
)

var (
	// Set this to a non-empty value in a consumer process
	// e.g. see how faktory_worker_go sets this.
	RandomProcessWid = ""
)

// The Client structure represents a thread-safe connection
// to a Faktory server.
type Client struct {
	Location string
	Options  *ClientData
	util.Pool
}

// ClientData is serialized to JSON and sent
// with the HELLO command.  PasswordHash is required
// if the server is not listening on localhost.
// The WID (worker id) must be random and unique
// for each worker process.  It can be a UUID, etc.
// Non-worker processes should leave WID empty.
//
// The other elements can be useful for debugging
// and are displayed on the Busy tab.
type ClientData struct {
	Hostname string   `json:"hostname"`
	Wid      string   `json:"wid"`
	Pid      int      `json:"pid"`
	Labels   []string `json:"labels"`
	// Hash is hex(sha256(password + nonce))
	PasswordHash string `json:"pwdhash"`
	// The protocol version used by this client.
	// The server can reject this connection if the version will not work
	// The server advertises its protocol version in the HI.
	Version int `json:"v"`
}

type Server struct {
	Network         string
	Address         string
	Password        string
	Timeout         time.Duration
	TLS             *tls.Config
	InitialPoolSize int
	MaxPoolSize     int
}

func (s *Server) Open() (*Client, error) {
	return Dial(s, s.Password)
}

func (s *Server) ReadFromEnv() error {
	val, ok := os.LookupEnv("FAKTORY_PROVIDER")
	if ok {
		if strings.Contains(val, ":") {
			return fmt.Errorf(`Error: FAKTORY_PROVIDER is not a URL. It is the name of the ENV var that contains the URL:

FAKTORY_PROVIDER=FOO_URL
FOO_URL=tcp://:mypassword@faktory.example.com:7419`)
		}

		uval, ok := os.LookupEnv(val)
		if ok {
			uri, err := url.Parse(uval)
			if err != nil {
				return err
			}
			s.Network = uri.Scheme
			s.Address = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
			if uri.User != nil {
				s.Password, _ = uri.User.Password()
			}
			return nil
		}
		return fmt.Errorf("FAKTORY_PROVIDER set to invalid value: %s", val)
	}

	uval, ok := os.LookupEnv("FAKTORY_URL")
	if ok {
		uri, err := url.Parse(uval)
		if err != nil {
			return err
		}
		s.Network = uri.Scheme
		s.Address = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
		if uri.User != nil {
			s.Password, _ = uri.User.Password()
		}
		return nil
	}

	return nil
}

func DefaultServer() *Server {
	return &Server{"tcp", "localhost:7419", "", 1 * time.Second, &tls.Config{}, 0, 1}
}

// Open connects to a Faktory server based on
// environment variable conventions:
//
// • Use FAKTORY_PROVIDER to point to a custom URL variable.
// • Use FAKTORY_URL as a catch-all default.
//
// Use the URL to configure any necessary password:
//
//    tcp://:mypassword@localhost:7419
//
// By default Open assumes localhost with no password
// which is appropriate for local development.
func Open() (*Client, error) {
	srv := DefaultServer()
	err := srv.ReadFromEnv()
	if err != nil {
		return nil, err
	}
	// Connect to default localhost
	return srv.Open()
}

func OpenPool(poolSize int) (*Client, error) {
	srv := DefaultServer()
	srv.MaxPoolSize = poolSize
	err := srv.ReadFromEnv()
	if err != nil {
		return nil, err
	}
	// Connect to default localhost
	return srv.Open()
}

// Dial connects to the remote faktory server.
//
//   client.Dial(client.Localhost, "topsecret")
//
func Dial(srv *Server, password string) (*Client, error) {
	clientData := emptyClientData()

	var err error
	var pool util.Pool
	dial := &net.Dialer{Timeout: srv.Timeout}
	if srv.Network == "tcp+tls" {
		pool, err = util.NewChannelPool(srv.InitialPoolSize, srv.MaxPoolSize, func() (util.Closeable, error) { return tls.DialWithDialer(dial, "tcp", srv.Address, srv.TLS) })
		if err != nil {
			return nil, err
		}
	} else {
		pool, err = util.NewChannelPool(srv.InitialPoolSize, srv.MaxPoolSize, func() (util.Closeable, error) {
			conn, err := dial.Dial(srv.Network, srv.Address)
			if err != nil {
				return nil, err
			}
			if x, ok := conn.(*net.TCPConn); ok {
				x.SetKeepAlive(false)
			}

			return conn, nil
		})
	}

	tcpConn, err := getTCPConn(pool)
	if err != nil {
		return nil, err
	}

	rdr := bufio.NewReader(tcpConn)

	line, err := readString(rdr)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if strings.HasPrefix(line, "HI ") {
		str := strings.TrimSpace(line)[3:]

		var hi map[string]interface{}
		err = json.Unmarshal([]byte(str), &hi)
		if err != nil {
			pool.Close()
			return nil, err
		}
		v, ok := hi["v"].(float64)
		if ok {
			if ExpectedProtocolVersion != int(v) {
				fmt.Println("Warning: server and client protocol versions out of sync:", v, ExpectedProtocolVersion)
			}
		}

		salt, ok := hi["s"].(string)
		if ok {
			iter := 1
			iterVal, ok := hi["i"]
			if ok {
				iter = int(iterVal.(float64))
			}

			clientData.PasswordHash = hash(password, salt, iter)
		}
	} else {
		pool.Close()
		return nil, fmt.Errorf("Expecting HI but got: %s", line)
	}

	data, err := json.Marshal(clientData)
	if err != nil {
		return nil, err
	}

	err = writeLine(bufio.NewWriter(tcpConn), "HELLO", data)
	if err != nil {
		pool.Close()
		return nil, err
	}

	err = ok(rdr)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return &Client{Options: clientData, Location: srv.Address, Pool: pool}, nil
}

func (c *Client) Close() error {
	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "END", nil); err != nil {
		return err
	}

	c.Pool.Close()
	return nil
}

func (c *Client) Ack(jid string) error {
	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "ACK", []byte(fmt.Sprintf(`{"jid":"%s"}`, jid))); err != nil {
		return err
	}

	return ok(bufio.NewReader(tcpConn))
}

func (c *Client) Push(job *Job) error {
	jobytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "PUSH", jobytes); err != nil {
		return err
	}
	return ok(bufio.NewReader(tcpConn))
}

func (c *Client) Fetch(q ...string) (*Job, error) {
	if len(q) == 0 {
		return nil, fmt.Errorf("Fetch must be called with one or more queue names")
	}

	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return nil, err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "FETCH", []byte(strings.Join(q, " "))); err != nil {
		return nil, err
	}

	data, err := readResponse(bufio.NewReader(tcpConn))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
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

// Fail notifies Faktory that a job failed with the given error.
// If backtrace is non-nil, it is assumed to be the output from
// runtime/debug.Stack().
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

	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return err
	}

	err = writeLine(bufio.NewWriter(tcpConn), "FAIL", failbytes)
	if err != nil {
		return err
	}

	return ok(bufio.NewReader(tcpConn))
}

func (c *Client) Flush() error {
	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "FLUSH", nil); err != nil {
		return err
	}

	return ok(bufio.NewReader(tcpConn))
}

func (c *Client) Info() (map[string]interface{}, error) {
	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return nil, err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), "INFO", nil); err != nil {
		return nil, err
	}

	data, err := readResponse(bufio.NewReader(tcpConn))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
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
	tcpConn, err := getTCPConn(c.Pool)
	if err != nil {
		return "", err
	}

	if err := writeLine(bufio.NewWriter(tcpConn), cmdline, nil); err != nil {
		return "", err
	}

	return readString(bufio.NewReader(tcpConn))
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
	client.Version = ExpectedProtocolVersion
	return client
}

func writeLine(w *bufio.Writer, op string, payload []byte) error {
	//util.Debugf("> %s %s", op, string(payload))

	_, err := w.Write([]byte(op))
	if payload != nil {
		if err == nil {
			_, err = w.Write([]byte(" "))
		}
		if err == nil {
			_, err = w.Write(payload)
		}
	}
	if err == nil {
		_, err = w.Write([]byte("\r\n"))
	}
	if err == nil {
		err = w.Flush()
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
	if val == nil {
		return "", nil
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
		if count == -1 {
			return nil, nil
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

func getTCPConn(pool util.Pool) (*net.TCPConn, error) {
	poolConn, err := pool.Get()
	if err != nil {
		return nil, err
	}
	defer poolConn.Close()

	tcpConn, validConn := poolConn.(*util.PoolConn).Closeable.(*net.TCPConn)
	if !validConn {
		return nil, fmt.Errorf("Invalid connection type")
	}

	return tcpConn, nil
}
