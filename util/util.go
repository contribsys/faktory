package util

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	LogInfo    = false
	LogDebug   = false
	LogVerbose = false
)

func Darwin() bool {
	b, _ := FileExists("/Applications")
	return b
}

func FileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func ReadLines(data []byte) ([]string, error) {
	var lines []string
	scan := bufio.NewScanner(bytes.NewReader(data))
	for scan.Scan() {
		lines = append(lines, scan.Text())
	}
	return lines, scan.Err()
}

//
// Logging functions
//

func SetLogLevel(level string) {
	if level == "info" {
		LogInfo = true
	}

	if level == "debug" {
		LogInfo = true
		LogDebug = true
	}

	if level == "verbose" {
		LogInfo = true
		LogDebug = true
		LogVerbose = true
	}
}

// Uh oh, not good but not worthy of process death
func Warn(msg string, args ...interface{}) {
	if len(args) > 0 {
		log.Printf(preamble('W')+msg+"\n", args...)
	} else {
		log.Println(preamble('W') + msg)
	}
}

// Typical logging output, the default level
func Info(msg string, args ...interface{}) {
	if LogInfo {
		if len(args) > 0 {
			log.Printf(preamble('I')+msg+"\n", args...)
		} else {
			log.Println(preamble('I') + msg)
		}
	}
}

// -l debug: Verbosity level which helps track down production issues
func Debug(msg string, args ...interface{}) {
	if LogDebug {
		if len(args) > 0 {
			log.Printf(preamble('D')+msg+"\n", args...)
		} else {
			log.Println(preamble('D') + msg)
		}
	}
}

// -l verbose: Very verbose for development purposes
func DebugDebug(msg string, args ...interface{}) {
	if LogVerbose {
		if len(args) > 0 {
			log.Printf(preamble('V')+msg+"\n", args...)
		} else {
			log.Println(preamble('V') + msg)
		}
	}
}

const (
	TimestampFormat = "2006-01-02T15:04:05.000000Z"
)

func preamble(lvl rune) string {
	return fmt.Sprintf("%c %s %d ", lvl, time.Now().UTC().Format(TimestampFormat), os.Getpid())
}
