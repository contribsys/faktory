package client

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	// util.LogInfo = true
	// util.LogDebug = true
	go stacks()
}

type specialError struct {
	Msg string
}

func (s *specialError) Error() string {
	return s.Msg
}

func TestClientOperations(t *testing.T) {
	cl, err := Open()
	assert.Error(t, err)
	assert.Nil(t, cl)

	err = os.Setenv("FAKTORY_PROVIDER", "tcp://localhost:7419")
	assert.NoError(t, err)

	cl, err = Open()
	assert.Error(t, err)
	assert.Nil(t, cl)

	withFakeServer(t, func(req, resp chan string, addr string) {
		err = os.Setenv("FAKTORY_PROVIDER", "MIKE_URL")
		assert.NoError(t, err)
		err = os.Setenv("MIKE_URL", "tcp://:foobar@"+addr)
		assert.NoError(t, err)

		resp <- "+OK\r\n"
		cl, err := Open()
		assert.NoError(t, err)
		assert.NotNil(t, cl)
		s := <-req
		assert.Contains(t, s, "HELLO")
		assert.Contains(t, s, "pwdhash")

		resp <- "+OK\r\n"
		err = cl.PauseQueues("foo", "bar")
		assert.NoError(t, err)
		assert.Contains(t, <-req, "QUEUE PAUSE foo bar")

		resp <- "+OK\r\n"
		err = cl.ResumeQueues("foo", "bar")
		assert.NoError(t, err)
		assert.Contains(t, <-req, "QUEUE RESUME foo bar")

		resp <- "+OK\r\n"
		res, err := cl.Beat("")
		assert.NoError(t, err)
		assert.Equal(t, "", res)
		assert.Contains(t, <-req, "BEAT")

		resp <- "+OK\r\n"
		err = cl.Kill(Retries, OfType("SomeJobType"))
		assert.NoError(t, err)
		assert.Contains(t, <-req, "MUTATE")

		job, err := cl.Fetch()
		assert.Error(t, err)
		assert.Nil(t, job)

		resp <- "$0\r\n\r\n"
		job, err = cl.Fetch("default")
		assert.NoError(t, err)
		assert.Nil(t, job)
		assert.Contains(t, <-req, "FETCH")

		resp <- "$2\r\n{}\r\n"
		jobs := []*Job{
			NewJob("foo", 1, 2),
			NewJob("foo", 1, 2),
		}

		result, err := cl.PushBulk(jobs)
		assert.NoError(t, err)
		assert.Contains(t, <-req, "PUSHB")
		assert.EqualValues(t, 0, len(result))

		resp <- "+OK\r\n"
		err = cl.Push(NewJob("foo", 1, 2))
		assert.NoError(t, err)
		assert.Contains(t, <-req, "PUSH")

		resp <- "+OK\r\n"
		err = cl.Ack("123456")
		assert.NoError(t, err)
		assert.Contains(t, <-req, "ACK")

		resp <- "+OK\r\n"
		err = cl.Fail("123456", &specialError{Msg: "Some error"}, debug.Stack())
		assert.NoError(t, err)
		assert.Contains(t, <-req, "FAIL")

		resp <- "$2\r\n{}\r\n"
		hash, err := cl.Info()
		assert.NoError(t, err)
		assert.NotNil(t, hash)
		assert.Contains(t, <-req, "INFO")

		resp <- "$36\r\n{\"faktory\":{\"queues\":{\"default\":2}}}\r\n"
		sizes, err := cl.QueueSizes()
		assert.NoError(t, err)
		assert.Equal(t, sizes["default"], uint64(2))
		assert.Contains(t, <-req, "INFO")

		resp <- "$39\r\n{\"faktory\":{\"queues\":{\"invalid\":null}}}\r\n"
		sizes, err = cl.QueueSizes()
		assert.Error(t, err)
		assert.Nil(t, sizes)
		assert.Contains(t, <-req, "INFO")

		err = cl.Close()
		assert.NoError(t, err)
		assert.Contains(t, <-req, "END")
	})
}

func withFakeServer(t *testing.T, fn func(chan string, chan string, string)) {
	binding := "localhost:44434"

	addr, err := net.ResolveTCPAddr("tcp", binding)
	assert.NoError(t, err)
	listener, err := net.ListenTCP("tcp", addr)
	assert.NoError(t, err)

	req := make(chan string, 1)
	resp := make(chan string, 1)

	go func() {
		conn, err := listener.Accept()
		assert.NoError(t, err)
		_ = conn.SetDeadline(time.Now().Add(1 * time.Second))
		_, _ = conn.Write([]byte("+HI {\"v\":2,\"s\":\"123\",\"i\":123}\r\n"))
		for {
			buf := bufio.NewReader(conn)
			line, err := buf.ReadString('\n')
			if err != nil {
				fmt.Println(err)
				conn.Close()
				break
			}
			// util.Infof("> %s", line)
			req <- line
			rsp := <-resp
			// util.Infof("< %s", rsp)
			_, _ = conn.Write([]byte(rsp))
		}
	}()

	fn(req, resp, binding)
	listener.Close()
}

func stacks() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	buf := make([]byte, 1<<20)
	<-sigs
	stacklen := runtime.Stack(buf, true)
	log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	os.Exit(-10)
}

func TestPasswordHashing(t *testing.T) {
	iterations := 1545
	pwd := "foobar"
	salt := "55104dc76695721d"

	result := hash(pwd, salt, iterations)
	assert.Equal(t, "6d877f8e5544b1f2598768f817413ab8a357afffa924dedae99eb91472d4ec30", result)
}
