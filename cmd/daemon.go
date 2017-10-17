package main

import (
	"fmt"

	"github.com/mperham/faktory/cli"
	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/util"
	"github.com/mperham/faktory/webui"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debug("Options", opts)

	// touch webui so it initializes
	webui.Password = ""

	s := server.NewServer(&server.ServerOptions{
		Binding:          opts.Binding,
		StorageDirectory: opts.StorageDirectory,
		ConfigDirectory:  opts.ConfigDirectory,
		Environment:      opts.Environment,
		DisableTls:       opts.DisableTls,
	})

	go cli.HandleSignals(s)

	err := s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
