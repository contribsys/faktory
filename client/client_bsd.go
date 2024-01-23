//go:build darwin || freebsd || netbsd || openbsd

package client

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/contribsys/faktory/util"
)

func RssKb() int64 {
	// TODO Submit a PR?
	cmd := exec.Command("ps", "-p", fmt.Sprintf("%d", os.Getpid()), "-o", "rss=") // nolint:gosec
	out, err := cmd.CombinedOutput()
	if err != nil {
		util.Warnf("Error gathering RSS/BSD: %v", err)
		return 0
	}
	val, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		util.Warnf("Error gathering RSS/BSD: %v", err)
		return 0
	}
	return int64(val)
}
