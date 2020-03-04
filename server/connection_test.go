package server

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnectionBasics(t *testing.T) {
	dc := dummyConnection()

	assert.NotNil(t, dc)

	err := dc.Ok()
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", output(dc))

	err = dc.Number(123)
	assert.NoError(t, err)
	assert.Equal(t, ":123\r\n", output(dc))

	err = dc.Result(nil)
	assert.NoError(t, err)
	assert.Equal(t, "$-1\r\n", output(dc))

	err = dc.Result([]byte("{some:jobjson}"))
	assert.NoError(t, err)
	assert.Equal(t, "$14\r\n{some:jobjson}\r\n", output(dc))

	err = dc.Error("bad command", fmt.Errorf("permission denied"))
	assert.NoError(t, err)
	assert.Equal(t, "-ERR permission denied\r\n", output(dc))

	dc.Close()
	assert.Equal(t, "", output(dc))
}

type TestingWriteCloser struct {
	*bufio.Writer
	output *bytes.Buffer
}

func (wc *TestingWriteCloser) Close() error {
	return wc.Flush()
}

func (wc *TestingWriteCloser) Output() string {
	wc.Flush()
	data := wc.output.String()
	wc.output.Reset()
	return data
}

func output(dc *Connection) string {
	tc := dc.conn.(*TestingWriteCloser)
	return tc.Output()
}

func dummyConnection() *Connection {
	writeBuffer := bytes.NewBuffer(make([]byte, 0))
	wc := &TestingWriteCloser{output: writeBuffer, Writer: bufio.NewWriter(writeBuffer)}

	return &Connection{
		client: dummyClientData(),
		conn:   wc,
		buf:    bufio.NewReader(strings.NewReader("")),
	}
}

func dummyClientData() *ClientData {
	return &ClientData{
		Hostname:      "foobar.example.com",
		Wid:           "123k1h23kh",
		Pid:           70086,
		Labels:        []string{"golang", "someapp"},
		StartedAt:     time.Now(),
		lastHeartbeat: time.Now(),
	}
}
