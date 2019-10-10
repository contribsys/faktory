package pool

import (
	"fmt"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/internal/pool"
)

type Pool struct {
	pool.Pool
}

func New(capacity int) (*Pool, error) {
	var p Pool
	var err error
	p.Pool, err = pool.NewChannelPool(0, capacity, func() (pool.Closeable, error) { return open() })
	return &p, err
}

func (p *Pool) Get() (*Client, error) {
	conn, err := p.Pool.Get()
	if err != nil {
		return nil, err
	}
	pc := conn.(*pool.PoolConn)
	client, ok := pc.Closeable.(*client.Client)
	if !ok {
		// Because we control the entire lifecycle of the pool, internally, this should never happen.
		panic(fmt.Sprintf("Connection is not a Faktory client instance: %+v", conn))
	}
	return &Client{Client: client, closable: conn}, nil
}

func (p *Pool) Close() error {
	p.Pool.Close()
	return nil
}

type Client struct {
	*client.Client
	closable pool.Closeable
}

func open() (*client.Client, error) {
	c, err := client.Open()
	return c, err
}

func (c *Client) Close() error {
	return c.closable.Close()
}

func (c *Client) Ack(jid string) error {
	err := c.Client.Ack(jid)
	if err != nil {
		c.markUnusable()
	}
	return err
}
func (c *Client) Push(job *client.Job) error {
	err := c.Client.Push(job)
	if err != nil {
		c.markUnusable()
	}
	return err
}
func (c *Client) Fetch(q ...string) (*client.Job, error) {
	job, err := c.Client.Fetch(q...)
	if err != nil {
		c.markUnusable()
	}
	return job, err
}
func (c *Client) Fail(jid string, err error, backtrace []byte) error {
	failErr := c.Client.Fail(jid, err, backtrace)
	if failErr != nil {
		c.markUnusable()
	}
	return failErr
}
func (c *Client) Flush() error {
	err := c.Client.Flush()
	if err != nil {
		c.markUnusable()
	}
	return err
}
func (c *Client) Info() (map[string]interface{}, error) {
	data, err := c.Client.Info()
	if err != nil {
		c.markUnusable()
	}
	return data, err
}
func (c *Client) Generic(cmdline string) (string, error) {
	s, err := c.Client.Generic(cmdline)
	if err != nil {
		c.markUnusable()
	}
	return s, err
}
func (c *Client) Beat() (string, error) {
	s, err := c.Client.Beat()
	if err != nil {
		c.markUnusable()
	}
	return s, err
}

func (c *Client) markUnusable() {
	c.closable.(*pool.PoolConn).MarkUnusable()
}
