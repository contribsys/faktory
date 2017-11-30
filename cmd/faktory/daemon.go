package main

import (
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debugf("Options: %v", opts)

	s, err := server.NewServer(&server.ServerOptions{
		Binding:          opts.Binding,
		StorageDirectory: opts.StorageDirectory,
		ConfigDirectory:  opts.ConfigDirectory,
		Environment:      opts.Environment,
	})
	if err != nil {
		util.Error("Unable to create a new server", err)
		return
	}

	webui.InitialSetup(s.Password)

	err = s.Boot()
	if err != nil {
		util.Error("Unable to start the server", err)
		return
	}

	go cli.HandleSignals(s)

	defer s.Stop(nil)
	s.Run()
}
