package cli

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mperham/worq"
	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
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
		log.Println(worq.Licensing)
	}
)

func SetupLogging(io io.Writer) {
	// Modern POSIX processes should log to STDOUT only
	// and use a smart init system to manage logging.  That
	// logging system should add things like PID, timestamp, etc
	// to the logging output so we don't add any context at all.
	log.SetOutput(io)
	log.SetFlags(0)
}

func ParseArguments() CmdOptions {
	defaults := CmdOptions{"localhost:7419", "development", false, "/etc/worq", "debug", "/var/run/worq.sock", "/var/run/worq"}

	log.Println(worq.Name, worq.Version)
	log.Println(fmt.Sprintf("Copyright © %d Contributed Systems LLC", time.Now().Year()))

	if StartupInfo != nil {
		StartupInfo()
	}

	flag.Usage = help
	flag.BoolVar(&defaults.TestConfig, "tc", false, "Test configuration and exit")
	flag.StringVar(&defaults.Binding, "b", "localhost:7419", "Network binding")
	flag.StringVar(&defaults.LogLevel, "l", "info", "Logging level (warn, info, debug, verbose)")
	flag.StringVar(&defaults.Environment, "e", "development", "Environment (development, staging, production, etc)")
	flag.StringVar(&defaults.StoragePath, "d", "/var/run/worq", "Storage directory")

	// undocumented on purpose, for testing only, we don't want people changing these
	// if possible
	flag.StringVar(&defaults.SocketPath, "s", "/var/run/worq.sock", "")
	flag.StringVar(&defaults.ConfigDirectory, "c", "/etc/worq", "")
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

	util.SetLogLevel(defaults.LogLevel)
	storage.DefaultPath = defaults.StoragePath

	return defaults
}

func help() {
	log.Println("-b [binding]\tNetwork binding (use :7419 to listen on all interfaces), default: localhost:7419")
	log.Println("-e [env]\tSet environment (development, staging, production), default: development")
	log.Println("-l [level]\tSet logging level (warn, info, debug, verbose), default: info")
	log.Println("-d [dir]\tStorage directory, default: /var/run/worq")
	log.Println("-tc\t\tTest configuration and exit")
	log.Println("-v\t\tShow version and license information")
	log.Println("-h\t\tThis help screen")
}

var (
	Term os.Signal = syscall.SIGTERM
	Hup  os.Signal = syscall.SIGHUP

	SignalHandlers = map[os.Signal]func(*worq.Server){
		Term:         exit,
		os.Interrupt: exit,
		//Hup:          reload,
	}
)

func HandleSignals(s *worq.Server) {
	signals := make(chan os.Signal)
	for k := range SignalHandlers {
		signal.Notify(signals, k)
	}

	fmt.Printf("Now listening at %s, press Ctrl-C to stop\n", s.Options.Binding)
	for {
		sig := <-signals
		util.Debug("Received signal %d", sig)
		funk := SignalHandlers[sig]
		funk(s)
	}
}

func exit(s *worq.Server) {
	util.Debug(worq.Name + " exiting")

	s.Stop(func() {
		util.Info("Goodbye")
		os.Exit(0)
	})
}
