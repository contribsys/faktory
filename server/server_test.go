package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func runServer(binding string, runner func()) {
	dir := strings.Replace(binding, ":", "_", 1)
	os.RemoveAll("/tmp/" + dir)
	opts := &ServerOptions{
		Binding:          binding,
		StorageDirectory: "/tmp/" + dir,
		ConfigDirectory:  os.ExpandEnv("$HOME/.faktory"),
	}
	s, err := NewServer(opts)
	if err != nil {
		panic(err)
	}
	err = s.Boot()
	if err != nil {
		panic(err)
	}

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()
	runner()
	s.Stop(nil)
}

func TestServerStart(t *testing.T) {
	t.Parallel()
	runServer("localhost:7420", func() {
		conn, err := net.DialTimeout("tcp", "localhost:7420", 1*time.Second)
		assert.NoError(t, err)
		buf := bufio.NewReader(conn)

		_, err = buf.ReadString('\n')
		assert.NoError(t, err)

		var client ClientData
		hs, err := os.Hostname()
		assert.NoError(t, err)
		client.Hostname = hs
		client.Pid = os.Getpid()
		client.Wid = strconv.FormatInt(rand.Int63(), 10)
		client.Labels = []string{"blue", "seven"}
		client.Version = 2

		val, err := json.Marshal(client)
		assert.NoError(t, err)

		conn.Write([]byte("HELLO "))
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
		_, err = buf.ReadString('\n')
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

		conn.Write([]byte(fmt.Sprintf("FAIL {\"jid\":\"%s\",\"message\":\"Invalid something\",\"errtype\":\"RuntimeError\"}\n", hash["jid"])))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte(fmt.Sprintf("ACK {\"jid\":\"%s\"}\n", hash["jid"])))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		conn.Write([]byte(fmt.Sprintf("INFO\n")))
		_, err = buf.ReadString('\n')
		assert.NoError(t, err)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)

		var stats map[string]interface{}
		err = json.Unmarshal([]byte(result), &stats)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(stats))

		conn.Write([]byte("END\n"))
		//result, err = buf.ReadString('\n')
		//assert.NoError(t, err)
		//assert.Equal(t, "OK\n", result)

		conn.Close()
	})

}

func TestPasswordHashing(t *testing.T) {
	iterations := 1545
	pwd := "foobar"
	salt := "55104dc76695721d"

	result := hash(pwd, salt, iterations)
	assert.Equal(t, "6d877f8e5544b1f2598768f817413ab8a357afffa924dedae99eb91472d4ec30", result)
}

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// 1550 µs per call with 5545 iterations
		iterations := 5545
		pwd := "foobar"
		salt := "55104dc76695721d"

		hash(pwd, salt, iterations)
	}
}
