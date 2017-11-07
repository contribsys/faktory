package server

import (
	"bufio"
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
	client *ClientWorker
	conn   io.WriteCloser
	buf    *bufio.Reader
}

func (c *Connection) Close() {
	c.conn.Close()
}

func (c *Connection) Error(cmd string, err error) error {
	x := internalError(err)
	//util.Error(fmt.Sprintf("Error processing line: %s", cmd), err, x.Stack)
	_, err = c.conn.Write([]byte("-ERR " + x.Error.Error() + "\r\n"))
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
