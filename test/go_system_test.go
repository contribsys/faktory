package tester

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	opts := cli.ParseArguments()
	util.InitLogger("info")

	dir := "/tmp/system.db"
	defer os.RemoveAll(dir)

	opts.ConfigDirectory = "."
	opts.StorageDirectory = dir
	s, stopper, err := cli.BuildServer(opts)
	if stopper != nil {
		defer stopper()
	}

	if err != nil {
		panic(err)
	}

	util.LogInfo = true

	go stacks()
	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}
	s.Register(webui.Subsystem(opts.WebBinding))

	go s.Run()

	// this is a worker process so we need to set the global WID before connecting
	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	each := 5000
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
	assert.EqualValues(t, 3*each, s.Store().TotalProcessed())
	assert.EqualValues(t, 3*(each/100), s.Store().TotalFailures())

	s.Stop(nil)
}

func pushAndPop(t *testing.T, count int) {
	time.Sleep(300 * time.Millisecond)
	client, err := client.Dial(client.DefaultServer(), "123456")
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

func pushJob(cl *client.Client, idx int) error {
	j := &client.Job{
		Jid:   util.RandomJid(),
		Queue: "default",
		Type:  "SomeJob",
		Args:  []interface{}{1, "string", 3},
	}
	return cl.Push(j)
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
