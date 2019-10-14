package client

import (
	"fmt"

	"github.com/contribsys/faktory/internal/pool"
)

type Pool struct {
	pool.Pool
}

func NewPool(capacity int) (*Pool, error) {
	var p Pool
	var err error
	p.Pool, err = pool.NewChannelPool(0, capacity, func() (pool.Closeable, error) { return Open() })
	return &p, err
}

func (p *Pool) Get() (*PoolClient, error) {
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
	return &PoolClient{Client: client, poolConn: pc}, nil
}

func (p *Pool) Close() error {
	p.Pool.Close()
	return nil
}

type PoolClient struct {
	*Client
	poolConn *pool.PoolConn
}

func (c *PoolClient) Close() error {
	return c.poolConn.Close()
}
