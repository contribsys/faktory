package client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolGetPut(t *testing.T) {
	p, err := NewPool(10)
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
		p.Put(cl)

		err = p.With(func(conn *Client) error {
			// Should be able to use the client like normal
			resp <- "+OK\r\n"
			res, err := cl.Beat()
			assert.NoError(t, err)
			assert.Equal(t, "", res)
			assert.Contains(t, <-req, "BEAT")
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestPoolConnectionError(t *testing.T) {
	p, err := NewPool(10)
	assert.NoError(t, err)
	assert.NotNil(t, p)

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

		// I can't figure out how to test this. I need the server to have a transport-level error, like the connection
		// being closed by the server, in order to test this.

		// fakeServer.Shutdown() ??
		// res,err = cl.Beat()
		// assert.Error(t, err)

		// p.Put(cl)

		// assert.Equal(t, p.Len(), 0)
	})
}

func TestPoolClosePool(t *testing.T) {
	p, err := NewPool(10)
	assert.NoError(t, err)
	assert.NotNil(t, p)

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
		p.Put(cl)

		// Closing the pool should close the client
		assert.Equal(t, p.Len(), 1)
		p.Close()
		assert.Equal(t, p.Len(), 0)

		resp <- "+OK\r\n"
		res, err = cl.Beat("")
		assert.Error(t, err)
		assert.Equal(t, "", res)

		// Pool should return an error when trying to get another client
		cl, err = p.Get()
		assert.Nil(t, cl)
		assert.Error(t, err)
	})
}
