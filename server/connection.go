package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"

	"github.com/mperham/faktory/util"
)

/*
 * Represents a connection to a faktory client.
 *
 * faktory reuses the same wire protocol as Redis: RESP.
 * It's a nice trade-off between human-readable and efficient.
 * Shout out to antirez for his nice design document on it.
 *
 * https://redis.io/topics/protocol
 */
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
	util.Error(fmt.Sprintf("Error processing line: %s", cmd), err, x.Stack)
	c.conn.Write([]byte("-ERR "))
	c.conn.Write([]byte(x.Error.Error()))
	c.conn.Write([]byte("\r\n"))
	return nil
}

func (c *Connection) Ok() error {
	c.conn.Write([]byte("+OK\r\n"))
	return nil
}

func (c *Connection) Number(val int) error {
	c.conn.Write([]byte(":"))
	c.conn.Write([]byte(strconv.Itoa(val)))
	c.conn.Write([]byte("\r\n"))
	return nil
}

func (c *Connection) Result(msg []byte) error {
	c.conn.Write([]byte("$"))
	c.conn.Write([]byte(strconv.Itoa(len(msg))))
	c.conn.Write([]byte("\r\n"))
	c.conn.Write(msg)
	c.conn.Write([]byte("\r\n"))
	return nil
}
