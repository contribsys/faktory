package server

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/contribsys/faktory/util"
)

func fetchPassword(configDir string) (string, error) {
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

	return "", nil
}
