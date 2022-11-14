package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/contribsys/faktory/manager"
)

// Represents a connection to a faktory client.
//
// faktory reuses the same wire protocol as Redis: RESP.
// It's a nice trade-off between human-readable and efficient.
// Shout out to antirez for his nice design document on it.
// https://redis.io/topics/protocol
type Connection struct {
	client *ClientData
	conn   io.WriteCloser
	buf    *bufio.Reader
	context.Context
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) Error(cmd string, err error) error {
	if re, ok := err.(manager.KnownError); ok {
		_, err = fmt.Fprintf(c.conn, "-%s\r\n", re.Error())
	} else {
		_, err = fmt.Fprintf(c.conn, "-ERR %s\r\n", err.Error())
	}
	return err
}

func (c *Connection) Ok() error {
	_, err := c.conn.Write([]byte("+OK\r\n"))
	return err
}

func (c *Connection) Number(val int) error {
	_, err := c.conn.Write([]byte(":" + strconv.Itoa(val) + "\r\n"))
	return err
}

func (c *Connection) Result(msg []byte) error {
	if msg == nil {
		_, err := c.conn.Write([]byte("$-1\r\n"))
		return err
	}

	_, err := c.conn.Write([]byte("$" + strconv.Itoa(len(msg)) + "\r\n"))
	if err != nil {
		return err
	}
	if msg != nil {
		_, err = c.conn.Write(msg)
		if err != nil {
			return err
		}
	}
	_, err = c.conn.Write([]byte("\r\n"))
	return err
}
