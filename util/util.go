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
)

var (
	LogInfo  = false
	LogDebug = false
	logg     = NewLogger("info", false)
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

func InitLogger(level string) {
	logg = NewLogger(level, true)
	if level == "info" {
		LogInfo = true
	}

	if level == "debug" {
		LogInfo = true
		LogDebug = true
	}
}

func Log() Logger {
	return logg
}

func Error(msg string, err error, stack []byte) {
	logg.Error(msg, reflect.TypeOf(err).Name(), err.Error())
	if stack != nil {
		logg.Error(string(stack))
	}
}

// Uh oh, not good but not worthy of process death
func Warn(args ...interface{}) {
	logg.Warn(args...)
}

func Warnf(msg string, args ...interface{}) {
	logg.Warnf(msg, args...)
}

// Typical logging output, the default level
func Info(args ...interface{}) {
	if LogInfo {
		logg.Info(args...)
	}
}

// Typical logging output, the default level
func Infof(msg string, args ...interface{}) {
	if LogInfo {
		logg.Infof(msg, args...)
	}
}

// -l debug: Verbosity level which helps track down production issues
func Debug(args ...interface{}) {
	if LogDebug {
		logg.Debug(args...)
	}
}

// -l debug: Verbosity level which helps track down production issues
func Debugf(msg string, args ...interface{}) {
	if LogDebug {
		logg.Debugf(msg, args...)
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

func Thens(tim time.Time) string {
	return tim.UTC().Format(TimestampFormat)
}

func Nows() string {
	return time.Now().UTC().Format(TimestampFormat)
}
