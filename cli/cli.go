package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"syscall"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

type CmdOptions struct {
	Binding          string
	Environment      string
	ConfigDirectory  string
	LogLevel         string
	StorageDirectory string
	DisableTls       bool
	Password         string
}

var (
	StartupInfo = func() {
		log.Println(faktory.Licensing)
	}
)

func ParseArguments() CmdOptions {
	defaults := CmdOptions{"localhost:7419", "development", "/etc/faktory", "info", "/var/lib/faktory/db", false, ""}

	log.SetFlags(0)
	log.Println(faktory.Name, faktory.Version)
	log.Println(fmt.Sprintf("Copyright © %d Contributed Systems LLC", time.Now().Year()))

	if StartupInfo != nil {
		StartupInfo()
	}

	flag.Usage = help
	flag.StringVar(&defaults.Binding, "b", "localhost:7419", "Network binding")
	flag.StringVar(&defaults.LogLevel, "l", "info", "Logging level (error, warn, info, debug)")
	flag.StringVar(&defaults.Environment, "e", "development", "Environment (development, staging, production, etc)")
	flag.BoolVar(&defaults.DisableTls, "no-tls", false, "Disable TLS, I don't want encryption")

	// undocumented on purpose, we don't want people changing these if possible
	flag.StringVar(&defaults.StorageDirectory, "d", "/var/lib/faktory/db", "Storage directory")
	flag.StringVar(&defaults.ConfigDirectory, "c", "/etc/faktory", "Config directory")
	versionPtr := flag.Bool("v", false, "Show version")
	flag.Parse()

	if *versionPtr {
		os.Exit(0)
	}

	if defaults.Environment == "development" {
		usr, _ := user.Current()
		dir := usr.HomeDir
		// development defaults to the user's home dir so everything is local and
		// permissions aren't a problem.
		if defaults.StorageDirectory == "/var/lib/faktory/db" {
			defaults.StorageDirectory = filepath.Join(dir, ".faktory/db")
		}
		if defaults.ConfigDirectory == "/etc/faktory" {
			defaults.ConfigDirectory = filepath.Join(dir, ".faktory")
		}
	}
	return defaults
}

func help() {
	log.Println("-b [binding]\tNetwork binding (use :7419 to listen on all interfaces), default: localhost:7419")
	log.Println("-e [env]\tSet environment (development, staging, production), default: development")
	log.Println("-l [level]\tSet logging level (warn, info, debug, verbose), default: info")
	log.Println("-no-tls\tDisable TLS for network sockets, I know what I'm doing")
	log.Println("-v\t\tShow version and license information")
	log.Println("-h\t\tThis help screen")
}

var (
	Term os.Signal = syscall.SIGTERM
	Hup  os.Signal = syscall.SIGHUP

	SignalHandlers = map[os.Signal]func(*server.Server){
		Term:         exit,
		os.Interrupt: exit,
		//Hup:          reload,
	}
)

func HandleSignals(s *server.Server) {
	signals := make(chan os.Signal, 1)
	for k := range SignalHandlers {
		signal.Notify(signals, k)
	}

	for {
		sig := <-signals
		util.Debugf("Received signal: %v", sig)
		funk := SignalHandlers[sig]
		funk(s)
	}
}

func exit(s *server.Server) {
	util.Debugf("%s shutting down", faktory.Name)

	s.Stop(func() {
		util.Info("Goodbye")
		os.Exit(0)
	})
}
