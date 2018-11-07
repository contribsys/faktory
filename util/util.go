package util

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"encoding/base64"
	"fmt"
	mathrand "math/rand"
	"os"
	"runtime"
	"time"
)

const (
	SIGHUP  = 0x1
	SIGINT  = 0x2
	SIGQUIT = 0x3
	SIGTERM = 0xF
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

// FileExists checks if given file exists
func FileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReadLines reads a whole file into memory
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

func Error(msg string, err error) {
	logg.WithError(err).Error(msg)
}

// Uh oh, not good but not worthy of process death
func Warn(arg string) {
	logg.Warn(arg)
}

func Warnf(msg string, args ...interface{}) {
	logg.Warnf(msg, args...)
}

// Typical logging output, the default level
func Info(arg string) {
	if LogInfo {
		logg.Info(arg)
	}
}

// Typical logging output, the default level
func Infof(msg string, args ...interface{}) {
	if LogInfo {
		logg.Infof(msg, args...)
	}
}

// Verbosity level helps track down production issues:
//  -l debug
func Debug(arg string) {
	if LogDebug {
		logg.Debug(arg)
	}
}

// Verbosity level helps track down production issues:
//  -l debug
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

	return base64.RawURLEncoding.EncodeToString(bytes)
}

const (
	// This is the canonical timestamp format used by Faktory.
	// Always UTC, lexigraphically sortable.  This is the best
	// timestamp format, accept no others.
	TimestampFormat = time.RFC3339Nano
)

func Thens(tim time.Time) string {
	return tim.UTC().Format(TimestampFormat)
}

func Nows() string {
	return time.Now().UTC().Format(TimestampFormat)
}

func ParseTime(str string) (time.Time, error) {
	return time.Parse(TimestampFormat, str)
}

func MemoryUsage() string {
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	mb := m.Sys / 1024 / 1024
	return fmt.Sprintf("%v MB", mb)
}

// Backtrace gathers a backtrace for the caller.
// Return a slice of up to N stack frames.
func Backtrace(size int) []string {
	pc := make([]uintptr, size)
	n := runtime.Callers(2, pc)
	if n == 0 {
		return []string{}
	}

	pc = pc[:n] // pass only valid pcs to runtime.CallersFrames
	frames := runtime.CallersFrames(pc)

	str := make([]string, size)
	count := 0

	// Loop to get frames.
	// A fixed number of pcs can expand to an indefinite number of Frames.
	for i := 0; i < size; i++ {
		frame, more := frames.Next()
		str[i] = fmt.Sprintf("in %s:%d %s", frame.File, frame.Line, frame.Function)
		count++
		if !more {
			break
		}
	}

	return str[0:count]
}
