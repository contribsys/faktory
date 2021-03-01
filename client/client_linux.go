package client

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func RssKb() int64 {
	path := "/proc/self/status"
	if _, err := os.Stat(path); err != nil {
		return 0
	}

	content, err := ioutil.ReadFile(path)
	if err != nil {
		return 0
	}

	lines := bytes.Split(content, []byte("\n"))
	for idx := range lines {
		if lines[idx][0] == 'V' {
			ls := string(lines[idx])
			if strings.Contains(ls, "VmRSS") {
				str := strings.Split(ls, ":")[1]
				intt, err := strconv.Atoi(str)
				if err != nil {
					return 0
				}
				return int64(intt)
			}
		}
	}
	return 0
}
