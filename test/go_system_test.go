package tester

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	opts := cli.ParseArguments()
	util.InitLogger("info")

	os.RemoveAll("/tmp/system.db")
	defer os.RemoveAll("/tmp/system.db")
	s, err := server.NewServer(&server.ServerOptions{
		Binding:          opts.Binding,
		StorageDirectory: "/tmp/system.db",
	})
	if err != nil {
		panic(err)
	}

	util.LogInfo = true

	go stacks()
	go cli.HandleSignals(s)

	go func() {
		err = s.Start()
		if err != nil {
			panic(err)
		}
	}()

	s.WaitUntilInitialized()

	each := 10000
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pushAndPop(t, each)
			util.Infof("Processed %d jobs in %v", 3*each, time.Since(start))
		}()
	}

	wg.Wait()
	s.Stop(nil)

	assert.Equal(t, int64(3*each), atomic.LoadInt64(&s.Stats.Processed))
	assert.Equal(t, int64(3*(each/100)), atomic.LoadInt64(&s.Stats.Failures))
}

func pushAndPop(t *testing.T, count int) {
	time.Sleep(300 * time.Millisecond)
	client, err := faktory.Dial(faktory.DefaultServer(), "123456")
	if err != nil {
		handleError(err)
		return
	}
	defer client.Close()

	sig, err := client.Beat()
	assert.Equal(t, "", sig)
	assert.NoError(t, err)

	util.Info("Pushing")
	for i := 0; i < count; i++ {
		if err = pushJob(client, i); err != nil {
			handleError(err)
			return
		}
	}
	util.Info("Popping")

	for i := 0; i < count; i++ {
		job, err := client.Fetch("default")
		if err != nil {
			handleError(err)
			return
		}
		if i%100 == 99 {
			err = client.Fail(job.Jid, os.ErrClosed, nil)
		} else {
			err = client.Ack(job.Jid)
		}
		if err != nil {
			handleError(err)
			return
		}
	}
	util.Info("Done")
	hash, err := client.Info()
	if err != nil {
		handleError(err)
		return
	}
	util.Infof("%v", hash)
}

func pushJob(client *faktory.Client, idx int) error {
	j := &faktory.Job{
		Jid:   util.RandomJid(),
		Queue: "default",
		Type:  "SomeJob",
		Args:  []interface{}{1, "string", 3},
	}
	return client.Push(j)
}

func stacks() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	buf := make([]byte, 1<<20)
	for {
		<-sigs
		stacklen := runtime.Stack(buf, true)
		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	}
}

func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
}
