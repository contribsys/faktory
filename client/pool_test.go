package client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolCloseConnection(t *testing.T) {
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

		// Should be able to use the client like normal
		resp <- "+OK\r\n"
		res, err := cl.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")

		// Closing the client should return it to the pool, not close the connection
		assert.NoError(t, cl.Close())

		resp <- "+OK\r\n"
		res, err = cl.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")

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
