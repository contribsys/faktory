package worq

import (
	"bufio"
	"net"

	"github.com/mperham/worq/util"
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

func (c *Connection) Error(cmd string, err error) error {
	x := internalError(err)
	util.Warn("Error processing line: %s", cmd)
	util.Error(err, x.Stack)
	c.conn.Write([]byte("ERR "))
	c.conn.Write([]byte(x.Error.Error()))
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
