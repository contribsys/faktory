package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mperham/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func init() {
	storage.DefaultPath = "../tmp"
	os.Mkdir("../tmp", os.FileMode(os.ModeDir|0755))
}

func runServer(runner func()) {
	os.RemoveAll("../tmp/localhost_7420.db")
	opts := &ServerOptions{
		Binding: "localhost:7420",
	}
	s := NewServer(opts)
	go func() {
		err := s.Start()
		if err != nil {
			fmt.Println(err)
		}
	}()
	// rocks takes a few ms to initialize
	time.Sleep(100 * time.Millisecond)
	runner()
}

func TestServerStart(t *testing.T) {
	t.Parallel()
	runServer(func() {
		conn, err := net.DialTimeout("tcp", "localhost:7420", 1*time.Second)
		assert.NoError(t, err)
		buf := bufio.NewReader(conn)

		conn.Write([]byte("AHOY pwd:123456 other:thing\n"))
		result, err := buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte("CMD foo\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "$23\r\n", result)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "ERR unknown command CMD\r\n", result)

		conn.Write([]byte("PUSH {\"jid\":\"12345678901234567890abcd\",\"class\":\"Thing\",\"args\":[123],\"queue\":\"default\"}\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte("POP default some other\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "$189\r\n", result)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)

		hash := make(map[string]interface{})
		err = json.Unmarshal([]byte(result), &hash)
		assert.NoError(t, err)
		//fmt.Println(hash)
		assert.Equal(t, "12345678901234567890abcd", hash["jid"])
		//assert.Equal(t, "{\"jid\":\"12345678901234567890abcd\",\"class\":\"Thing\",\"args\":[123],\"queue\":\"default\"}\n", result)

		conn.Write([]byte(fmt.Sprintf("ACK %s\n", hash["jid"])))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte("END\n"))
		//result, err = buf.ReadString('\n')
		//assert.NoError(t, err)
		//assert.Equal(t, "OK\n", result)

		conn.Close()
	})

}
