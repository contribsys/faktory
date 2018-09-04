package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
)

// Read all config files in:
//   /etc/faktory/conf.d/*.toml (in production)
//   ~/.faktory/conf.d/*.toml (in development)
func readConfig(cdir string, env string) (map[string]interface{}, error) {
	hash := map[string]interface{}{}

	globs := []string{
		fmt.Sprintf("%s/conf.d/*.toml", cdir),
	}

	for _, glob := range globs {
		matches, err := filepath.Glob(glob)
		if err != nil {
			return nil, err
		}

		for _, file := range matches {
			util.Debugf("Reading configuration in %s", file)
			fileBytes, err := ioutil.ReadFile(file)
			if err != nil {
				return nil, err
			}
			err = toml.Unmarshal(fileBytes, &hash)
			if err != nil {
				util.Warnf("Unable to parse TOML file at %s", file)
				return nil, err
			}
		}
	}

	util.Debug("Merged configuration")
	util.Debugf("%v", hash)
	return hash, nil
}

func main() {
	log.SetFlags(0)
	log.Println(client.Name, client.Version)
	log.Println(fmt.Sprintf("Copyright © %d Contributed Systems LLC", time.Now().Year()))
	log.Println(client.Licensing)

	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debugf("Options: %v", opts)

	globalConfig, err := readConfig(opts.ConfigDirectory, opts.Environment)
	if err != nil {
		util.Error("Error in configuration", err)
		return
	}

	pwd, err := fetchPassword(opts.ConfigDirectory, opts.Environment)
	if err != nil {
		util.Error("Invalid password configuration", err)
		return
	}

	sock := fmt.Sprintf("%s/redis.sock", opts.StorageDirectory)
	s, err := server.NewServer(&server.ServerOptions{
		Binding:          opts.CmdBinding,
		StorageDirectory: opts.StorageDirectory,
		ConfigDirectory:  opts.ConfigDirectory,
		Environment:      opts.Environment,
		RedisSock:        sock,
		Password:         pwd,
		GlobalConfig:     globalConfig,
	})
	if err != nil {
		util.Error("Unable to create a new server", err)
		return
	}

	stopper, err := storage.BootRedis(opts.StorageDirectory, sock)
	if err != nil {
		util.Error("Unable to boot Redis", err)
		return
	}
	defer stopper()

	err = s.Boot()
	if err != nil {
		util.Error("Unable to boot the command server", err)
		return
	}
	uiopts := webui.DefaultOptions()
	uiopts.Binding = opts.WebBinding
	uiopts.Password = pwd

	ui := webui.NewWeb(s, uiopts)
	stopui, err := ui.Run()
	if err != nil {
		util.Error("Unable to start the UI", err)
		return
	}
	defer stopui()

	go cli.HandleSignals(s)

	s.Run()
}

func fetchPassword(configDir string, env string) (string, error) {
	val, ok := os.LookupEnv("FAKTORY_PASSWORD")
	if ok {
		return val, nil
	}

	pwd := configDir + "/password"
	exists, err := util.FileExists(pwd)
	if err != nil {
		return "", err
	}
	if exists {
		data, err := ioutil.ReadFile(pwd)
		if err != nil {
			util.Error("Unable to read file "+pwd, err)
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	}

	if env == "production" && !skip() {
		return "", fmt.Errorf("Faktory requires a password to be set in production mode, see the Security wiki page")
	}

	return "", nil
}

func skip() bool {
	val, ok := os.LookupEnv("FAKTORY_SKIP_PASSWORD")
	return ok && (val == "1" || val == "true" || val == "yes")
}
