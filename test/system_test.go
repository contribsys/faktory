package tester

import (
	"fmt"
	"os"
	"testing"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
)

func TestSystem(t *testing.T) {
	cli.SetupLogging(os.Stdout)
	opts := cli.ParseArguments()
	s := worq.NewServer(opts.Binding)

	storage.DefaultPath = "./system.db"

	go cli.HandleSignals(s)
	go pushAndPop()

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func pushAndPop() {
	defer os.Exit(0)

	client, err := worq.Dial("localhost", 7419, &worq.Options{Pwd: "123456"})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	for i := 0; i < 1000; i++ {
		fmt.Println("Pushing")
		if err = pushJob(client, i); err != nil {
			fmt.Println(err)
			return
		}
	}

	for i := 0; i < 1000; i++ {
		job, err := client.Pop("default")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("Processing %s\n", job.Jid)
		err = client.Ack(job.Jid)
		if err != nil {
			fmt.Println(err)
			return
		}
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
