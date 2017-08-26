package faktory

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mperham/faktory/util"
	"github.com/stretchr/testify/assert"
)

func init() {
	util.LogInfo = true
}

func TestClientOperations(t *testing.T) {
	cl, err := Open()
	assert.Error(t, err)
	assert.Nil(t, cl)

	withServer(t, func(req, resp chan string, addr string) {
		err = os.Setenv("FAKTORY_PROVIDER", "MIKE_URL")
		assert.NoError(t, err)
		err = os.Setenv("MIKE_URL", "tcp://"+addr)
		assert.NoError(t, err)

		resp <- "+OK\r\n"
		cl, err := Open()
		assert.NoError(t, err)
		assert.NotNil(t, cl)
		assert.Contains(t, <-req, "AHOY")

		resp <- "+OK\r\n"
		res, err := cl.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")
	})
}

func withServer(t *testing.T, fn func(chan string, chan string, string)) {
	binding := "localhost:44434"

	addr, err := net.ResolveTCPAddr("tcp", binding)
	assert.NoError(t, err)
	listener, err := net.ListenTCP("tcp", addr)
	assert.NoError(t, err)

	req := make(chan string, 1)
	resp := make(chan string, 1)

	go func() {
		conn, err := listener.Accept()
		assert.NoError(t, err)
		conn.SetDeadline(time.Now().Add(1 * time.Second))
		for {
			buf := bufio.NewReader(conn)
			line, err := buf.ReadString('\n')
			if err != nil {
				conn.Close()
				break
			}
			//util.Infof("> %s", line)
			req <- line
			rsp := <-resp
			//util.Infof("< %s", rsp)
			conn.Write([]byte(rsp))
		}
	}()

	fn(req, resp, binding)
	listener.Close()
}
