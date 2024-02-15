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

	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func runServer(binding string, runner func()) {
	dir := fmt.Sprintf("/tmp/%s", strings.Replace(binding, ":", "_", 1))
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/test.sock", dir)
	stopper, err := storage.Boot(dir, sock)
	if err != nil {
		panic(err)
	}
	defer stopper()

	opts := &ServerOptions{
		Binding:          binding,
		StorageDirectory: dir,
		RedisSock:        sock,
		ConfigDirectory:  os.ExpandEnv("test/.faktory"),
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
	runServer("localhost:4477", func() {
		conn, err := net.DialTimeout("tcp", "localhost:4477", 1*time.Second)
		assert.NoError(t, err)
		buf := bufio.NewReader(conn)

		_, err = buf.ReadString('\n')
		assert.NoError(t, err)

		var client ClientData
		hs, err := os.Hostname()
		assert.NoError(t, err)
		client.Hostname = hs
		client.Pid = os.Getpid()
		client.Wid = strconv.FormatInt(rand.Int63(), 10) //nolint:gosec
		client.Labels = []string{"blue", "seven"}
		client.Version = 2

		val, err := json.Marshal(client)
		assert.NoError(t, err)

		_, _ = conn.Write([]byte("HELLO "))
		_, _ = conn.Write(val)
		_, _ = conn.Write([]byte("\r\n"))
		result, err := buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("CMD foo\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "-ERR unknown command CMD\r\n", result)

		_, _ = conn.Write([]byte("QUEUE REMOVE frobnoz\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("PUSH {\"jid\":\"12345678901234567890abcd\",\"jobtype\":\"Thing\",\"args\":[123],\"queue\":\"default\"}\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("FETCH default some other\n"))
		_, err = buf.ReadString('\n')
		assert.NoError(t, err)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Regexp(t, "12345678901234567890abcd", result)
		assert.Regexp(t, "\"retry\":", result)

		hash := make(map[string]interface{})
		err = json.Unmarshal([]byte(result), &hash)
		assert.NoError(t, err)
		// fmt.Println(hash)
		assert.Equal(t, "12345678901234567890abcd", hash["jid"])
		// assert.Equal(t, "{\"jid\":\"12345678901234567890abcd\",\"class\":\"Thing\",\"args\":[123],\"queue\":\"default\"}\n", result)

		_, _ = fmt.Fprintf(conn, "FAIL {\"jid\":%q,\"message\":\"Invalid something\",\"errtype\":\"RuntimeError\"}\n", hash["jid"])
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = fmt.Fprintf(conn, "ACK {\"jid\":%q}\n", hash["jid"])
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("INFO\n"))
		_, err = buf.ReadString('\n')
		assert.NoError(t, err)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)

		var stats map[string]interface{}
		err = json.Unmarshal([]byte(result), &stats)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(stats))

		_, _ = fmt.Fprintf(conn, "BEAT {\"wid\":%q}\n", client.Wid)
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("QUEUE REMOVE default\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("FLUSH\n"))
		result, err = buf.ReadString('\n')
		assert.NoError(t, err)
		assert.Equal(t, "+OK\r\n", result)

		_, _ = conn.Write([]byte("END\n"))
		// result, err = buf.ReadString('\n')
		// assert.NoError(t, err)
		// assert.Equal(t, "OK\n", result)

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
