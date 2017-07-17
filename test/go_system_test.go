package tester

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	opts := cli.ParseArguments()
	util.InitLogger("debug")

	storage.DefaultPath = "../tmp"
	defer os.RemoveAll("../tmp/system.db")
	s := worq.NewServer(&worq.ServerOptions{Binding: opts.Binding, StoragePath: "system.db"})

	util.LogDebug = true
	util.LogInfo = true

	go stacks()
	go cli.HandleSignals(s)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			pushAndPop()
		}()
	}

	go func() {
		wg.Wait()
		s.Stop(nil)
	}()

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
	assert.Equal(t, int64(30000), s.Processed)
	assert.Equal(t, int64(300), s.Failures)
}

func pushAndPop() {
	time.Sleep(100 * time.Millisecond)
	client, err := worq.Dial(&worq.ClientOptions{Pwd: "123456"})
	if err != nil {
		handleError(err)
		return
	}
	defer client.Close()

	util.Info("Pushing")
	for i := 0; i < 10000; i++ {
		if err = pushJob(client, i); err != nil {
			handleError(err)
			return
		}
	}
	util.Info("Popping")

	for i := 0; i < 10000; i++ {
		job, err := client.Pop("default")
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
	util.Info(hash)
}

func pushJob(client *worq.Client, idx int) error {
	j := &worq.Job{
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
