package main

import (
	"fmt"
	"os"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
)

func main() {
	cli.SetupLogging(os.Stdout)
	opts := cli.ParseArguments()
	s := worq.NewServer(opts.Binding)

	go cli.HandleSignals(s)

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
