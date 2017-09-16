package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mperham/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func init() {
	storage.DefaultPath = "/tmp"
	os.Mkdir("/tmp", os.FileMode(os.ModeDir|0755))
}

func runServer(runner func()) {
	os.RemoveAll("/tmp/localhost_7420.db")
	opts := &ServerOptions{
		Binding: "localhost:7420",
	}
	s := NewServer(opts)
	go func() {
		err := s.Start()
		if err != nil {
			panic(err)
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

		var client ClientWorker
		hs, err := os.Hostname()
		assert.NoError(t, err)
		client.Hostname = hs
		client.Pid = os.Getpid()
		client.Wid = strconv.FormatInt(rand.Int63(), 10)
		client.Labels = []string{"blue", "seven"}
		client.Salt = "123456"
		// password is "123456" also
		client.PasswordHash = "958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"

		val, err := json.Marshal(client)
		assert.NoError(t, err)

		conn.Write([]byte("AHOY "))
		conn.Write(val)
		conn.Write([]byte("\r\n"))
		result, err := buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte("CMD foo\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "-ERR Unknown command CMD\r\n", result)

		conn.Write([]byte("PUSH {\"jid\":\"12345678901234567890abcd\",\"jobtype\":\"Thing\",\"args\":[123],\"queue\":\"default\"}\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte("FETCH default some other\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Regexp(t, "12345678901234567890abcd", result)

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
