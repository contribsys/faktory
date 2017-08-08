package main

import (
	"fmt"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/cli"
	"github.com/mperham/faktory/util"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debug("Options", opts)

	s := faktory.NewServer(&faktory.ServerOptions{Binding: opts.Binding})

	go cli.HandleSignals(s)

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
