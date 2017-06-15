package worq

import (
	"bufio"
	"net"
)

type Connection struct {
	ident string
	conn  net.Conn
	buf   *bufio.Reader
}

func (c *Connection) Identity() string {
	return c.ident
}

func (c *Connection) Close() {
	c.conn.Close()
}

func (c *Connection) Error(err error) error {
	c.conn.Write([]byte("ERR "))
	c.conn.Write([]byte(err.Error()))
	c.conn.Write([]byte("\n"))
	return nil
}

func (c *Connection) Ok() error {
	c.conn.Write([]byte("OK\n"))
	return nil
}

func (c *Connection) Result(msg []byte) error {
	c.conn.Write(msg)
	c.conn.Write([]byte("\n"))
	return nil
}
