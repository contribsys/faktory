package util

import (
	"fmt"
	"os"
	"time"
)

// colors.
const (
	red    = 31
	green  = 32
	yellow = 33
	blue   = 34
)

type Level int

const (
	InvalidLevel Level = iota - 1
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

var colors = [...]int{
	DebugLevel: green,
	InfoLevel:  blue,
	WarnLevel:  yellow,
	ErrorLevel: red,
	FatalLevel: red,
}

var lvlPrefix = [...]string{
	DebugLevel: "D",
	InfoLevel:  "I",
	WarnLevel:  "W",
	ErrorLevel: "E",
	FatalLevel: "F",
}

var (
	LogInfo  = false
	LogDebug = false
	logg     = os.Stdout
	colorize = isTTY(logg.Fd())
)

const (
	TimeFormat = "2006-01-02T15:04:05.000Z"
)

func llog(lvl Level, msg string) {
	prefix := lvlPrefix[lvl]
	ts := time.Now().UTC().Format(TimeFormat)

	if colorize {
		color := colors[lvl]
		fmt.Fprintf(logg, "\033[%dm%s\033[0m %s %s\n", color, prefix, ts, msg)
	} else {
		fmt.Fprintf(logg, "%s %s %s\n", prefix, ts, msg)
	}
}

// This generic logging interface hide
// a Logrus logger or another impl
type Logger interface {
	Debug(arg string)
	Debugf(format string, args ...interface{})
	Info(arg string)
	Infof(format string, args ...interface{})
	Warn(arg string)
	Warnf(format string, args ...interface{})
	Error(arg string)
	Errorf(format string, args ...interface{})

	// Log and terminate process (unrecoverable)
	Fatal(arg string)

	// Log with fmt.Printf-like formatting and terminate process (unrecoverable)
	Fatalf(format string, args ...interface{})
}

//
// Logging functions
//

func InitLogger(level string) {
	if level == "info" {
		LogInfo = true
	}

	if level == "debug" {
		LogInfo = true
		LogDebug = true
	}
}

func Error(msg string, err error) {
	llog(ErrorLevel, fmt.Sprintf("%s: %s", msg, err))
}

// Uh oh, not good but not worthy of process death
func Warn(arg string) {
	llog(WarnLevel, arg)
}

func Warnf(msg string, args ...interface{}) {
	llog(WarnLevel, fmt.Sprintf(msg, args...))
}

// Typical logging output, the default level
func Info(arg string) {
	if LogInfo {
		llog(InfoLevel, arg)
	}
}

// Typical logging output, the default level
func Infof(msg string, args ...interface{}) {
	if LogInfo {
		llog(InfoLevel, fmt.Sprintf(msg, args...))
	}
}

// Verbosity level helps track down production issues:
//  -l debug
func Debug(arg string) {
	if LogDebug {
		llog(DebugLevel, arg)
	}
}

// Verbosity level helps track down production issues:
//  -l debug
func Debugf(msg string, args ...interface{}) {
	if LogDebug {
		llog(DebugLevel, fmt.Sprintf(msg, args...))
	}
}
