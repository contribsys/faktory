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

	"github.com/contribsys/faktory/internal/pool"
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
	Labels           = []string{"golang"}
)

// Dialer is the interface for creating a specialized net.Conn.
type Dialer interface {
	Dial(network, addr string) (c net.Conn, err error)
}

// The Client structure represents a thread-unsafe connection
// to a Faktory server.  It is recommended to use a connection pool
// of Clients in a multi-threaded process.  See faktory_worker_go's
// internal connection pool for example.
type Client struct {
	Location string
	Options  *ClientData
	rdr      *bufio.Reader
	wtr      *bufio.Writer
	conn     net.Conn
	poolConn *pool.PoolConn
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

	// this can be used by proxies to route the connection.
	// it is ignored by Faktory.
	Username string `json:"username"`

	// Hash is hex(sha256(password + nonce))
	PasswordHash string `json:"pwdhash"`

	// The protocol version used by this client.
	// The server can reject this connection if the version will not work
	// The server advertises its protocol version in the HI.
	Version int `json:"v"`
}

type Server struct {
	Network  string
	Address  string
	Username string
	Password string
	Timeout  time.Duration
	TLS      *tls.Config
}

// OpenWithDialer creates a *Client with the dialer.
func (s *Server) OpenWithDialer(dialer Dialer) (*Client, error) {
	return DialWithDialer(s, s.Password, dialer)
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
				s.Username = uri.User.Username()
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
			return fmt.Errorf("cannot parse value of FAKTORY_URL environment variable: %w", err)
		}

		s.Network = uri.Scheme
		s.Address = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
		if uri.User != nil {
			s.Username = uri.User.Username()
			s.Password, _ = uri.User.Password()
		}
		return nil
	}

	return nil
}

func DefaultServer() *Server {
	return &Server{"tcp", "localhost:7419", "", "", 1 * time.Second, &tls.Config{MinVersion: tls.VersionTLS12}}
}

// Open connects to a Faktory server based on
// environment variable conventions:
//
// • Use FAKTORY_PROVIDER to point to a custom URL variable.
// • Use FAKTORY_URL as a catch-all default.
//
// Use the URL to configure any necessary password:
//
//	tcp://:mypassword@localhost:7419
//
// By default Open assumes localhost with no password
// which is appropriate for local development.
func Open() (*Client, error) {
	srv := DefaultServer()
	if err := srv.ReadFromEnv(); err != nil {
		return nil, fmt.Errorf("cannot read configuration from env: %w", err)
	}
	// Connect to default localhost
	return srv.Open()
}

// OpenWithDialer connects to a Faktory server
// following the same conventions as Open but
// instead uses dialer as the transport.
func OpenWithDialer(dialer Dialer) (*Client, error) {
	srv := DefaultServer()
	if err := srv.ReadFromEnv(); err != nil {
		return nil, fmt.Errorf("cannot read configuration from env: %w", err)
	}
	// Connect to default localhost
	return srv.OpenWithDialer(dialer)
}

// Dial connects to the remote faktory server with
// a Dialer reflecting the value of srv.Network; i.e.,
// a *tls.Dialer if "tcp+tls" and a *net.Dialer if
// not.
//
//	client.Dial(client.Localhost, "topsecret")
func Dial(srv *Server, password string) (*Client, error) {
	d := &net.Dialer{Timeout: srv.Timeout}
	dialer := Dialer(d)
	if srv.Network == "tcp+tls" {
		dialer = &tls.Dialer{NetDialer: d, Config: srv.TLS}
	}
	return dial(srv, password, dialer)
}

// DialWithDialer connects to the faktory server
func DialWithDialer(srv *Server, password string, dialer Dialer) (*Client, error) {
	return dial(srv, password, dialer)
}

type HIv2 struct {
	V int    `json:"v"`           // version, should be 2
	I int    `json:"i,omitempty"` // iterations
	S string `json:"s,omitempty"` // salt
}

// dial connects to the remote faktory server.
func dial(srv *Server, password string, dialer Dialer) (*Client, error) {
	client := emptyClientData()
	client.Username = srv.Username

	var err error
	var conn net.Conn

	conn, err = dialer.Dial("tcp", srv.Address)
	if err != nil {
		return nil, err
	}

	if x, ok := conn.(*net.TCPConn); ok {
		_ = x.SetKeepAlive(true)
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

		var hi HIv2
		err = util.JsonUnmarshal([]byte(str), &hi)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if ExpectedProtocolVersion != hi.V {
			util.Infof("Warning: server and client protocol versions out of sync: want %d, got %d", ExpectedProtocolVersion, hi.V)
		}

		salt := hi.S
		if salt != "" {
			iter := hi.I
			client.PasswordHash = hash(password, salt, iter)
		}
	} else {
		conn.Close()
		return nil, fmt.Errorf("expecting HI but got: %s", line)
	}

	data, err := json.Marshal(client)
	if err != nil {
		return nil, fmt.Errorf("cannot JSON marshal: %w", err)
	}

	if err := writeLine(w, "HELLO", data); err != nil {
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
	_ = writeLine(c.wtr, "END", nil)
	return c.conn.Close()
}

func (c *Client) Ack(jid string) error {
	err := c.writeLine(c.wtr, "ACK", []byte(fmt.Sprintf(`{"jid":%q}`, jid)))
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

// Result is map[JID]ErrorMessage
func (c *Client) PushBulk(jobs []*Job) (map[string]string, error) {
	jobBytes, err := json.Marshal(jobs)
	if err != nil {
		return nil, err
	}
	err = c.writeLine(c.wtr, "PUSHB", jobBytes)
	if err != nil {
		return nil, err
	}
	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	results := map[string]string{}
	err = util.JsonUnmarshal(data, &results)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (c *Client) Push(job *Job) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}
	err = c.writeLine(c.wtr, "PUSH", jobBytes)
	if err != nil {
		return err
	}
	return c.ok(c.rdr)
}

func (c *Client) Fetch(q ...string) (*Job, error) {
	if len(q) == 0 {
		return nil, fmt.Errorf("Fetch must be called with one or more queue names")
	}

	err := c.writeLine(c.wtr, "FETCH", []byte(strings.Join(q, " ")))
	if err != nil {
		return nil, err
	}

	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var job Job
	err = util.JsonUnmarshal(data, &job)
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
	err = c.writeLine(c.wtr, "FAIL", failbytes)
	if err != nil {
		return err
	}
	return c.ok(c.rdr)
}

func (c *Client) Flush() error {
	err := c.writeLine(c.wtr, "FLUSH", nil)
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

// List queues explicitly or use "*" to remove all known queues
func (c *Client) RemoveQueues(names ...string) error {
	err := c.writeLine(c.wtr, "QUEUE REMOVE", []byte(strings.Join(names, " ")))
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

// List queues explicitly or use "*" to pause all known queues
func (c *Client) PauseQueues(names ...string) error {
	err := c.writeLine(c.wtr, "QUEUE PAUSE", []byte(strings.Join(names, " ")))
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

// List queues explicitly or use "*" to resume all known queues
func (c *Client) ResumeQueues(names ...string) error {
	err := c.writeLine(c.wtr, "QUEUE RESUME", []byte(strings.Join(names, " ")))
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

// deprecated, this returns an untyped map.
// use CurrentState() instead which provides strong typing
func (c *Client) Info() (map[string]interface{}, error) {
	util.Info("client.Info() is deprecated, use client.CurrentState() instead")

	err := c.writeLine(c.wtr, "INFO", nil)
	if err != nil {
		return nil, err
	}

	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var cur map[string]interface{}
	err = util.JsonUnmarshal(data, &cur)
	if err != nil {
		return nil, err
	}

	return cur, nil
}

func (c *Client) CurrentState() (*FaktoryState, error) {
	err := c.writeLine(c.wtr, "INFO", nil)
	if err != nil {
		return nil, err
	}

	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var cur FaktoryState
	err = util.JsonUnmarshal(data, &cur)
	if err != nil {
		return nil, err
	}
	return &cur, nil
}

func (c *Client) QueueSizes() (map[string]uint64, error) {
	state, err := c.CurrentState()
	if err != nil {
		return nil, err
	}
	return state.Data.Queues, nil
}

func (c *Client) Generic(cmdline string) (string, error) {
	err := c.writeLine(c.wtr, cmdline, nil)
	if err != nil {
		return "", err
	}

	return c.readString(c.rdr)
}

/*
 * The first arg to Beat allows a worker process to report its current lifecycle state
 * to Faktory. All worker processes must follow the same basic lifecycle:
 *
 * (startup) -> "" -> "quiet" -> "terminate"
 *
 * Quiet allows the process to finish its current work without fetching any new work.
 * Terminate means the process should exit within X seconds, usually ~30 seconds.
 */
func (c *Client) Beat(args ...string) (string, error) {
	state := ""
	if len(args) > 0 {
		state = args[0]
	}
	hash := map[string]interface{}{}
	hash["wid"] = RandomProcessWid
	hash["rss_kb"] = RssKb()

	if state != "" {
		hash["current_state"] = state
	}
	data, err := json.Marshal(hash)
	if err != nil {
		return "", err
	}
	cmd := fmt.Sprintf("BEAT %s", data)
	val, err := c.Generic(cmd)
	if val == "OK" {
		return "", nil
	}
	return val, err
}

func (c *Client) writeLine(wtr *bufio.Writer, op string, payload []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := writeLine(wtr, op, payload)
	if err != nil {
		c.markUnusable()
	}
	return err
}

func (c *Client) readResponse(rdr *bufio.Reader) ([]byte, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	data, err := readResponse(rdr)
	if err != nil {
		if _, ok := err.(*ProtocolError); !ok {
			c.markUnusable()
		}
	}
	return data, err
}

func (c *Client) ok(rdr *bufio.Reader) error {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	err := ok(rdr)
	if err != nil {
		if _, ok := err.(*ProtocolError); !ok {
			c.markUnusable()
		}
	}
	return err
}

func (c *Client) readString(rdr *bufio.Reader) (string, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	s, err := readString(rdr)
	if err != nil {
		if _, ok := err.(*ProtocolError); !ok {
			c.markUnusable()
		}
	}
	return s, err
}

func (c *Client) markUnusable() {
	if c.poolConn == nil {
		// if this client was not created as part of a pool,
		// this call becomes a no-op
		return
	}
	c.poolConn.MarkUnusable()
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
	client.Labels = Labels
	client.Version = ExpectedProtocolVersion
	return client
}

func writeLine(wtr *bufio.Writer, op string, payload []byte) error {
	// util.Debugf("> %s %s", op, string(payload))

	_, err := wtr.WriteString(op)
	if payload != nil {
		if err == nil {
			_, err = wtr.WriteString(" ")
		}
		if err == nil {
			_, err = wtr.Write(payload)
		}
	}
	if err == nil {
		_, err = wtr.WriteString("\r\n")
	}
	if err == nil {
		err = wtr.Flush()
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

	return fmt.Errorf("invalid response: %s", string(val))
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
		// util.Debugf("< %s%s", string(chr), string(line))
		// util.Debugf("< %s", string(buff))
		return buff, nil
	case '-':
		return nil, &ProtocolError{msg: string(line)}
	default:
		// util.Debugf("< %s%s", string(chr), string(line))
		return line, nil
	}
}

func hash(pwd, salt string, iterations int) string {
	data := []byte(pwd + salt)
	hash := sha256.Sum256(data)
	if iterations > 1 {
		for i := 1; i < iterations; i++ {
			hash = sha256.Sum256(hash[:])
		}
	}
	return fmt.Sprintf("%x", hash)
}
