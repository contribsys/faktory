package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
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
}

func (c *Connection) Close() {
	c.conn.Close()
}

func (c *Connection) Error(cmd string, err error) error {
	re, ok := err.(*taggedError)
	if ok {
		_, err = c.conn.Write([]byte(fmt.Sprintf("-%s\r\n", re.Error())))
	} else {
		_, err = c.conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
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
