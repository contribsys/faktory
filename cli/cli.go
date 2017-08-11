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

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/server"
	"github.com/mperham/faktory/storage"
	"github.com/mperham/faktory/util"
)

type CmdOptions struct {
	Binding         string
	Environment     string
	TestConfig      bool
	ConfigDirectory string
	LogLevel        string
	SocketPath      string
	StoragePath     string
}

var (
	StartupInfo = func() {
		log.Println(faktory.Licensing)
	}
)

func ParseArguments() CmdOptions {
	defaults := CmdOptions{"localhost:7419", "development", false, "/etc/faktory", "info", "/var/run/faktory.sock", "/var/run/faktory"}

	log.SetFlags(0)
	log.Println(faktory.Name, faktory.Version)
	log.Println(fmt.Sprintf("Copyright © %d Contributed Systems LLC", time.Now().Year()))

	if StartupInfo != nil {
		StartupInfo()
	}

	flag.Usage = help
	flag.BoolVar(&defaults.TestConfig, "tc", false, "Test configuration and exit")
	flag.StringVar(&defaults.Binding, "b", "localhost:7419", "Network binding")
	flag.StringVar(&defaults.LogLevel, "l", "info", "Logging level (error, warn*, info, debug)")
	flag.StringVar(&defaults.Environment, "e", "development", "Environment (development*, staging, production, etc)")
	flag.StringVar(&defaults.StoragePath, "d", "/var/run/faktory", "Storage directory")

	// undocumented on purpose, for testing only, we don't want people changing these
	// if possible
	flag.StringVar(&defaults.SocketPath, "s", "/var/run/faktory.sock", "")
	flag.StringVar(&defaults.ConfigDirectory, "c", "/etc/faktory", "")
	helpPtr := flag.Bool("help", false, "You're looking at it")
	help2Ptr := flag.Bool("h", false, "You're looking at it")
	versionPtr := flag.Bool("v", false, "Show version")
	flag.Parse()

	if *helpPtr || *help2Ptr {
		help()
		os.Exit(0)
	}

	if *versionPtr {
		os.Exit(0)
	}

	storage.DefaultPath = defaults.StoragePath
	if defaults.Environment == "development" {
		usr, _ := user.Current()
		dir := usr.HomeDir
		storage.DefaultPath = filepath.Join(dir, ".faktory")
	}
	err := os.Mkdir(storage.DefaultPath, os.FileMode(os.ModeDir|0755))
	if err != nil && !os.IsExist(err) {
		log.Fatalf("Cannot create faktory's data directory: %v", err)
	}
	return defaults
}

func help() {
	log.Println("-b [binding]\tNetwork binding (use :7419 to listen on all interfaces), default: localhost:7419")
	log.Println("-e [env]\tSet environment (development, staging, production), default: development")
	log.Println("-l [level]\tSet logging level (warn, info, debug, verbose), default: info")
	log.Println("-d [dir]\tStorage directory, default: /var/run/faktory")
	log.Println("-tc\t\tTest configuration and exit")
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
	signals := make(chan os.Signal)
	for k := range SignalHandlers {
		signal.Notify(signals, k)
	}

	util.Info("Now listening at ", s.Options.Binding, ", press Ctrl-C to stop")
	for {
		sig := <-signals
		util.Debug("Received signal %d", sig)
		funk := SignalHandlers[sig]
		funk(s)
	}
}

func exit(s *server.Server) {
	util.Debug(faktory.Name + " shutting down")

	s.Stop(func() {
		util.Info("Goodbye")
		os.Exit(0)
	})
}
