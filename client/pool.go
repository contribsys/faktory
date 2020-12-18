package client

import (
	"fmt"

	"github.com/contribsys/faktory/internal/pool"
)

type Pool struct {
	pool.Pool
}

// NewPool creates a new Pool object through which multiple clients will be managed on your behalf.
//
// Call Get() to retrieve a client instance and Put() to return it to the pool. If you do not call
// Put(), the connection will be leaked, and the pool will stop working once it hits capacity.
//
// Do NOT call Close() on the client, as the lifecycle is managed internally.
//
// The dialer clients in this pool use is determined by the URI scheme in FAKTORY_PROVIDER.
func NewPool(capacity int) (*Pool, error) {
	return newPool(capacity, func() (pool.Closeable, error) { return Open() })
}

// NewPoolWithDialer creates a new Pool object similar to NewPool but clients will use the
// provided dialer instead of default ones.
func NewPoolWithDialer(capacity int, dialer Dialer) (*Pool, error) {
	fn := func() (pool.Closeable, error) { return OpenWithDialer(dialer) }
	return newPool(capacity, fn)
}

// newPool creates a *Pool channel with the provided capacity and opener.
func newPool(capacity int, opener pool.Factory) (*Pool, error) {
	var p Pool
	var err error
	p.Pool, err = pool.NewChannelPool(0, capacity, opener)
	return &p, err
}

// Get retrieves a Client from the pool. This Client is created, internally, by calling
// the Open() function, and has all the same behaviors.
func (p *Pool) Get() (*Client, error) {
	conn, err := p.Pool.Get()
	if err != nil {
		return nil, err
	}
	pc := conn.(*pool.PoolConn)
	client, ok := pc.Closeable.(*Client)
	if !ok {
		// Because we control the entire lifecycle of the pool, internally, this should never happen.
		panic(fmt.Sprintf("Connection is not a Faktory client instance: %+v", conn))
	}
	client.poolConn = pc
	return client, nil
}

// Put returns a client to the pool.
func (p *Pool) Put(client *Client) {
	client.poolConn.Close()
}

func (p *Pool) With(fn func(conn *Client) error) error {
	conn, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Put(conn)
	return fn(conn)
}
