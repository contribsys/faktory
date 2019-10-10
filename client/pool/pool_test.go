package pool

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	p, err := New(10)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	cl, err := p.Get()
	assert.Error(t, err)
	assert.Nil(t, cl)

	withFakeServer(t, func(req, resp chan string, addr string) {
		err = os.Setenv("FAKTORY_PROVIDER", "MIKE_URL")
		assert.NoError(t, err)
		err = os.Setenv("MIKE_URL", "tcp://:foobar@"+addr)
		assert.NoError(t, err)

		resp <- "+OK\r\n"
		cl, err := p.Get()
		assert.NoError(t, err)
		assert.NotNil(t, cl)
		s := <-req
		assert.Contains(t, s, "HELLO")
		assert.Contains(t, s, "pwdhash")

		// Should be able to use the client like normal
		resp <- "+OK\r\n"
		res, err := cl.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")

		// // Closing the client should return it to the pool, not close the connection
		// assert.NoError(t, cl.Close())
		//
		// resp <- "+OK\r\n"
		// res, err = cl.Beat()
		// assert.NoError(t, err)
		// assert.Equal(t, "", res)
		// assert.Contains(t, <-req, "BEAT")

		// I can't figure out how to test this. I need the server to send an error response so the
		// pool client can detect it and mark the connection as unusable

		// An error response should mark the client as unusable, thus closing it when returned to the pool.
		// resp <- "-ERR bad request\r\n"
		// res, err = cl.Beat()
		// assert.Error(t, err)
		// assert.Equal(t, "", res)
		// assert.Contains(t, <-req, "bad request")

		// assert.NoError(t, cl.Close())

		// should fail because it is closed
		// res, err = cl.Beat()
		// assert.Error(t, err)
	})

	withFakeServer(t, func(req, resp chan string, addr string) {
		err = os.Setenv("FAKTORY_PROVIDER", "MIKE_URL")
		assert.NoError(t, err)
		err = os.Setenv("MIKE_URL", "tcp://:foobar@"+addr)
		assert.NoError(t, err)

		resp <- "+OK\r\n"
		cl, err := p.Get()
		assert.NoError(t, err)
		assert.NotNil(t, cl)
		s := <-req
		assert.Contains(t, s, "HELLO")
		assert.Contains(t, s, "pwdhash")

		// Should be able to use the client like normal
		resp <- "+OK\r\n"
		res, err := cl.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")

		// Return the client to the pool
		cl.Close()

		// Closing the pool should close the client
		p.Close()

		resp <- "+OK\r\n"
		res, err = cl.Beat()
		assert.Error(t, err)

		// Pool should return an error when trying to get another client
		cl, err = p.Get()
		assert.Nil(t, cl)
		assert.Error(t, err)
	})
}

func withFakeServer(t *testing.T, fn func(chan string, chan string, string)) {
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
		conn.Write([]byte("+HI {\"v\":2,\"s\":\"123\",\"i\":123}\r\n"))
		for {
			buf := bufio.NewReader(conn)
			line, err := buf.ReadString('\n')
			if err != nil {
				fmt.Println(err)
				conn.Close()
				break
			}
			// util.Infof("> %s", line)
			req <- line
			rsp := <-resp
			// util.Infof("< %s", rsp)
			conn.Write([]byte(rsp))
		}
	}()

	fn(req, resp, binding)
	listener.Close()
}
