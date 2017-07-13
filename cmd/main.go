package main

import (
	"fmt"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	worq.InitializeLogger(opts.LogLevel)

	s := worq.NewServer(&worq.ServerOptions{Binding: opts.Binding})

	go cli.HandleSignals(s)

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
