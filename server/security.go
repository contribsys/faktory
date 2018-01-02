package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/contribsys/faktory/util"
)

func fetchPassword(opts *ServerOptions) (string, error) {
	val, ok := os.LookupEnv("FAKTORY_PASSWORD")
	if ok {
		return val, nil
	}

	configDir := opts.ConfigDirectory
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

	if opts.Environment == "production" && !skip() {
		return "", fmt.Errorf("Faktory requires a password to be set in production mode, see the Security wiki page")
	}

	return "", nil
}

func skip() bool {
	val, ok := os.LookupEnv("FAKTORY_SKIP_PASSWORD")
	return ok && (val == "1" || val == "true" || val == "yes")
}
