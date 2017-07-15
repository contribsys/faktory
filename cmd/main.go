package main

import (
	"fmt"

	"github.com/mperham/worq"
	"github.com/mperham/worq/cli"
	"github.com/mperham/worq/util"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debug("Options", opts)

	s := worq.NewServer(&worq.ServerOptions{Binding: opts.Binding})

	go cli.HandleSignals(s)

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
