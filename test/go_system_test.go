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

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	cli.SetupLogging(os.Stdout)
	opts := cli.ParseArguments()

	storage.DefaultPath = "."

	s := worq.NewServer(&worq.ServerOptions{Binding: opts.Binding, StoragePath: "./system.db"})

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
		assert.Equal(t, 3000, s.Processed)
		assert.Equal(t, 30, s.Failures)
		fmt.Println(s.Processed, s.Failures)
		os.Exit(0)
	}()

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func pushAndPop() {
	client, err := worq.Dial(&worq.ClientOptions{Pwd: "123456"})
	if err != nil {
		handleError(err)
		return
	}
	defer client.Close()

	util.Debug("Pushing")
	for i := 0; i < 100000; i++ {
		if err = pushJob(client, i); err != nil {
			handleError(err)
			return
		}
	}
	util.Debug("Popping")

	for i := 0; i < 100000; i++ {
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
	hash, err := client.Info()
	if err != nil {
		handleError(err)
		return
	}
	if len(hash) != 5 {
		fmt.Println("Info", hash)
	}
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
