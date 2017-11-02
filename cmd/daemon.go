package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
)

func readConfig(cdir string, env string) (map[string]interface{}, error) {
	hash := map[string]interface{}{}

	files := []string{
		fmt.Sprintf("%s/config.toml", cdir),
	}

	if env == "development" {
		files = append(files, os.ExpandEnv("$HOME/.faktory/config.toml"))
	}

	for _, file := range files {
		exists, err := util.FileExists(file)
		if err != nil {
			return nil, err
		}

		if !exists {
			continue
		}

		util.Debugf("Using configuration in %s", file)
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
	return hash, nil
}

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger(opts.LogLevel)
	util.Debug("Options", opts)

	globalConfig, err := readConfig(opts.ConfigDirectory, opts.Environment)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(globalConfig)

	s, err := server.NewServer(&server.ServerOptions{
		Binding:          opts.Binding,
		StorageDirectory: opts.StorageDirectory,
		ConfigDirectory:  opts.ConfigDirectory,
		Environment:      opts.Environment,
		DisableTls:       opts.DisableTls,
		Configuration:    globalConfig,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	webui.InitialSetup(s.Password)

	go cli.HandleSignals(s)

	err = s.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
}
