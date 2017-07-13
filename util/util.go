package util

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"encoding/base64"
	mathrand "math/rand"
	"os"
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"
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

func Error(msg string, err error, stack []byte) {
	log.Println(preamble('E'), msg, reflect.TypeOf(err).Name(), err.Error())
	if stack != nil {
		log.Println(string(stack))
	}
}

// Uh oh, not good but not worthy of process death
func Warn(msg string, args ...interface{}) {
	if len(args) > 0 {
		log.Printf("%s %s %v\n", preamble('W'), msg, args)
	} else {
		log.Println(preamble('W') + msg)
	}
}

// Typical logging output, the default level
func Info(msg string, args ...interface{}) {
	if LogInfo {
		if len(args) > 0 {
			log.Printf("%s %s %v\n", preamble('I'), msg, args)
		} else {
			log.Println(preamble('I') + msg)
		}
	}
}

// -l debug: Verbosity level which helps track down production issues
func Debug(msg string, args ...interface{}) {
	if LogDebug {
		if len(args) > 0 {
			log.Printf("%s %s %v\n", preamble('D'), msg, args)
		} else {
			log.Println(preamble('D'), msg)
		}
	}
}

func RandomJid() string {
	bytes := make([]byte, 12)
	_, err := cryptorand.Read(bytes)
	if err != nil {
		mathrand.Read(bytes)
	}

	return base64.URLEncoding.EncodeToString(bytes)
}

const (
	TimestampFormat = "2006-01-02T15:04:05.000000Z"
)

func preamble(lvl rune) string {
	return "" //fmt.Sprintf("%c %s %d ", lvl, time.Now().UTC().Format(TimestampFormat), os.Getpid())
}

func Thens(tim time.Time) string {
	return tim.UTC().Format(TimestampFormat)
}

func Nows() string {
	return time.Now().UTC().Format(TimestampFormat)
}
