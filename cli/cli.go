package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type CliOptions struct {
	CmdBinding       string
	WebBinding       string
	Environment      string
	ConfigDirectory  string
	LogLevel         string
	StorageDirectory string
}

func ParseArguments() CliOptions {

	fenv := os.Getenv("FAKTORY_ENV")
	if fenv == "" {
		fenv = "development"
	}

	defaults := CliOptions{"localhost:7419", "localhost:7420", fenv, "/etc/faktory", "info", "/var/lib/faktory/db"}

	flag.Usage = help
	flag.StringVar(&defaults.WebBinding, "w", "localhost:7420", "WebUI binding")
	flag.StringVar(&defaults.CmdBinding, "b", "localhost:7419", "Network binding")
	flag.StringVar(&defaults.LogLevel, "l", "info", "Logging level (error, warn, info, debug)")
	flag.StringVar(&defaults.Environment, "e", fenv, "Environment (development, staging, production)")

	// undocumented on purpose, we don't want people changing these if possible
	flag.StringVar(&defaults.StorageDirectory, "d", "/var/lib/faktory/db", "Storage directory")
	flag.StringVar(&defaults.ConfigDirectory, "c", "/etc/faktory", "Config directory")
	versionPtr := flag.Bool("v", false, "Show version")
	flag.Parse()

	if *versionPtr {
		os.Exit(0)
	}

	if defaults.Environment == "development" {
		envdir, ok := os.LookupEnv("HOME")
		var dir string
		if ok && envdir != "" {
			dir = envdir
		}
		usr, err := user.Current()
		if err == nil {
			dir = usr.HomeDir
		}
		// development defaults to the user's home dir so everything is local and
		// permissions aren't a problem.
		if defaults.StorageDirectory == "/var/lib/faktory/db" {
			defaults.StorageDirectory = filepath.Join(dir, ".faktory", "db")
		}
		if defaults.ConfigDirectory == "/etc/faktory" {
			defaults.ConfigDirectory = filepath.Join(dir, ".faktory")
		}
	} else if defaults.Environment == "staging" || defaults.Environment == "production" {
	} else {
		help()
		log.Println("")
		log.Fatalf(`Invalid environment "%s": legal values are development, staging or production`, defaults.Environment)
	}
	return defaults
}

func help() {
	log.Println("-b [binding]\tNetwork binding (use :7419 to listen on all interfaces), default: localhost:7419")
	log.Println("-w [binding]\tWeb UI binding (use :7420 to listen on all interfaces), default: localhost:7420")
	log.Println("-e [env]\tSet environment (development, staging, production), default: development")
	log.Println("-l [level]\tSet logging level (error, warn, info, debug), default: info")
	log.Println("-v\t\tShow version and license information")
	log.Println("-h\t\tThis help screen")
}

var (
	Term os.Signal = syscall.SIGTERM
	Hup  os.Signal = syscall.SIGHUP
	Info os.Signal = syscall.SIGTTIN

	SignalHandlers = map[os.Signal]func(*server.Server){
		Term:         exit,
		os.Interrupt: exit,
		Hup:          reload,
		Info:         threadDump,
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

func reload(s *server.Server) {
	util.Debugf("%s reloading", client.Name)

	globalConfig, err := readConfig(s.Options.ConfigDirectory, s.Options.Environment)
	if err != nil {
		util.Warnf("Unable to reload config: %v", err)
		return
	}

	s.Options.GlobalConfig = globalConfig
	s.Reload()
}

func exit(s *server.Server) {
	util.Infof("%s shutting down", client.Name)

	close(s.Stopper())
}

func threadDump(s *server.Server) {
	util.DumpProcessTrace()
}

func BuildServer(opts *CliOptions) (*server.Server, func(), error) {
	globalConfig, err := readConfig(opts.ConfigDirectory, opts.Environment)
	if err != nil {
		return nil, nil, err
	}

	pwd, err := fetchPassword(globalConfig, opts.Environment)
	if err != nil {
		return nil, nil, err
	}

	sock := fmt.Sprintf("%s/redis.sock", opts.StorageDirectory)
	stopper, err := storage.Boot(opts.StorageDirectory, sock)
	if err != nil {
		return nil, stopper, err
	}

	// allow binding config element if no CLI arg spec'd:
	// [faktory]
	//   binding = "0.0.0.0:7419"
	if opts.CmdBinding == "localhost:7419" {
		opts.CmdBinding = stringConfig(globalConfig, "faktory", "binding", "localhost:7419")
	}

	sopts := &server.ServerOptions{
		Binding:          opts.CmdBinding,
		StorageDirectory: opts.StorageDirectory,
		ConfigDirectory:  opts.ConfigDirectory,
		Environment:      opts.Environment,
		RedisSock:        sock,
		GlobalConfig:     globalConfig,
		Password:         pwd,
		PoolSize:         server.DefaultMaxPoolSize,
	}

	// don't log config hash until fetchPassword has had a chance to scrub the password value
	util.Debug("Merged configuration")
	util.Debugf("%v", globalConfig)

	s, err := server.NewServer(sopts)
	if err != nil {
		return nil, stopper, err
	}

	return s, stopper, nil
}

func stringConfig(cfg map[string]interface{}, subsys string, elm string, defval string) string {
	if mapp, ok := cfg[subsys]; ok {
		if mappp, ok := mapp.(map[string]interface{}); ok {
			if val, ok := mappp[elm]; ok {
				if sval, ok := val.(string); ok {
					return sval
				}
			}
		}
	}
	return defval
}

// Read all config files in:
//
//	/etc/faktory/conf.d/*.toml (in production)
//	~/.faktory/conf.d/*.toml (in development)
//
// They are read in alphabetical order.
// File contents are shallow merged, a latter file
// can override a value from an earlier file.
func readConfig(cdir string, env string) (map[string]interface{}, error) {
	hash := map[string]interface{}{}

	globs := []string{
		fmt.Sprintf("%s/conf.d/*.toml", cdir),
	}

	combined := []byte{}
	for idx := range globs {
		matches, err := filepath.Glob(globs[idx])
		if err != nil {
			return nil, err
		}

		for fidx := range matches {
			file := matches[fidx]
			util.Debugf("Reading configuration in %s", file)
			fileBytes, err := os.ReadFile(file)
			if err != nil {
				return nil, err
			}
			fileBytes = append(fileBytes, "\n"...)
			combined = append(combined, fileBytes...)
		}
	}

	err := toml.Unmarshal(combined, &hash)
	if err != nil {
		util.Warnf("Unable to parse configs")
		return nil, err
	}
	return hash, nil
}

// Expects a TOML file like:
//
// [faktory]
// password = "foobar" # or...
// password = "/run/secrets/my_faktory_password"
func fetchPassword(cfg map[string]interface{}, env string) (string, error) {
	password := ""

	// allow the password to be injected via ENV rather than committed
	// to filesystem.  Note if this value starts with a /, then it is
	// considered a pointer to a file on the filesystem with the password
	// value, e.g. FAKTORY_PASSWORD=/run/secrets/my_faktory_password.
	val, ok := os.LookupEnv("FAKTORY_PASSWORD")
	if ok {
		password = val
	} else {

		val := stringConfig(cfg, "faktory", "password", "")
		if val != "" {
			password = val

			// clear password so we can log it safely
			x := cfg["faktory"].(map[string]interface{})
			x["password"] = "********"
		}
	}

	if env != "development" && !skip() && password == "" {
		ok, _ := util.FileExists("/etc/faktory/password")
		if ok {
			//nolint:gosec
			password = "/etc/faktory/password"
		}
	}

	if strings.HasPrefix(password, "/") {
		// allow password value to point to a file.
		// this is how Docker secrets work.
		data, err := os.ReadFile(password)
		if err != nil {
			return "", err
		}

		password = strings.TrimSpace(string(data))
	}

	if env != "development" && !skip() && password == "" {
		return "", fmt.Errorf("faktory requires a password to be set in staging or production, see the Security wiki page")
	}

	return password, nil
}

func skip() bool {
	val, ok := os.LookupEnv("FAKTORY_SKIP_PASSWORD")
	return ok && (val == "1" || val == "true" || val == "yes")
}
