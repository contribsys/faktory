package main

import (
	"log"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
)

func logPreamble() {
	log.SetFlags(0)
	log.Println(client.Name, client.Version)
	log.Printf("Copyright © %d Contributed Systems LLC", time.Now().Year())
	log.Println("Licensed under the GNU Affero Public License 3.0")
}

func main() {
	logPreamble()

	opts := cli.ParseArguments()
	util.InitLogger(opts.LogLevel)
	util.Debugf("Options: %v", opts)

	s, stopper, err := cli.BuildServer(&opts)
	if stopper != nil {
		defer stopper()
	}

	if err != nil {
		util.Error("Unable to create Faktory server", err)
		return
	}

	err = s.Boot()
	if err != nil {
		util.Error("Unable to boot the command server", err)
		return
	}

	s.Register(webui.Subsystem(opts.WebBinding))

	go cli.HandleSignals(s)
	go func() {
		_ = s.Run()
	}()

	<-s.Stopper()
	s.Stop(nil)
}
