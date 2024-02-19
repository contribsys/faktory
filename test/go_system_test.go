package tester

import (
	"context"
	"crypto/tls"
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

	opts.ConfigDirectory = "./cfg"
	opts.StorageDirectory = dir
	s, stopper, err := cli.BuildServer(&opts)
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

	go func() {
		_ = s.Run()
	}()

	// this is a worker process so we need to set the global WID before connecting
	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32) //nolint:gosec

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
	bg := context.Background()
	assert.EqualValues(t, 3*each, int(s.Store().TotalProcessed(bg)))
	assert.EqualValues(t, 3*(each/100), int(s.Store().TotalFailures(bg)))

	s.Stop(nil)
}

func pushAndPop(t *testing.T, count int) {
	time.Sleep(300 * time.Millisecond)
	s := &client.Server{
		Network:  "tcp+tls",
		Address:  "localhost:7419",
		Username: "",
		Password: "",
		Timeout:  1 * time.Second,
		TLS: &tls.Config{InsecureSkipVerify: true, // nolint:gosec
			MinVersion: tls.VersionTLS12},
	}
	cl, err := client.Dial(s, "123456")
	if err != nil {
		handleError(err)
		return
	}
	defer cl.Close()

	sig, err := cl.Beat()
	assert.Equal(t, "", sig)
	assert.NoError(t, err)

	util.Info("Pushing")
	for i := 0; i < count; i++ {
		if err = pushJob(cl, i); err != nil {
			handleError(err)
			return
		}
	}
	util.Info("Popping")

	for i := 0; i < count; i++ {
		job, err := cl.Fetch("default")
		if err != nil {
			handleError(err)
			return
		}
		assert.NotNil(t, job)
		if i%100 == 99 {
			err = cl.Fail(job.Jid, os.ErrClosed, nil)
		} else {
			err = cl.Ack(job.Jid)
		}
		if err != nil {
			handleError(err)
			return
		}
	}
	err = pushBulk(cl)
	if err != nil {
		handleError(err)
		return
	}

	util.Info("Done")
	hash, err := cl.Info()
	if err != nil {
		handleError(err)
		return
	}
	util.Infof("%v", hash)
}

func pushJob(cl *client.Client, idx int) error {
	return cl.Push(client.NewJob("SomeJob", 1, "string", 3))
}

func pushBulk(cl *client.Client) error {
	job1 := client.NewJob("SomeJob", 1, "string", 4)
	job2 := client.NewJob("SomeJob", 1, "string", 3)
	job1.Type = "" // create error
	result, err := cl.PushBulk([]*client.Job{job1, job2})
	if _, ok := result[job1.Jid]; !ok {
		return fmt.Errorf("Expected result to contain JID %s: %v", job1.Jid, result)
	}
	return err
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
	fmt.Println(strings.ReplaceAll(err.Error(), "\n", ""))
}
