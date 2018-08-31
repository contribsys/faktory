package util

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	alog "github.com/apex/log"
)

// colors.
const (
	red    = 31
	green  = 32
	yellow = 33
	blue   = 34
)

// Colors mapping.
var Colors = [...]int{
	alog.DebugLevel: green,
	alog.InfoLevel:  blue,
	alog.WarnLevel:  yellow,
	alog.ErrorLevel: red,
	alog.FatalLevel: red,
}

// Strings mapping.
var Strings = [...]string{
	alog.DebugLevel: "D",
	alog.InfoLevel:  "I",
	alog.WarnLevel:  "W",
	alog.ErrorLevel: "E",
	alog.FatalLevel: "F",
}

type LogHandler struct {
	mu     sync.Mutex
	writer io.Writer
	tty    bool
}

const (
	TimeFormat = "2006-01-02T15:04:05.000Z"
)

func (h *LogHandler) HandleLog(e *alog.Entry) error {
	color := Colors[e.Level]
	level := Strings[e.Level]
	names := e.Fields.Names()
	ts := time.Now().UTC().Format(TimeFormat)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.tty {
		fmt.Fprintf(h.writer, "\033[%dm%s\033[0m %s ", color, level, ts)
	} else {
		fmt.Fprintf(h.writer, "%s %s ", level, ts)
	}

	for _, name := range names {
		fmt.Fprintf(h.writer, "%s=%v ", name, e.Fields.Get(name))
	}
	fmt.Fprintln(h.writer, e.Message)

	return nil
}

func NewLogger(level string, production bool) Logger {
	alog.SetHandler(&LogHandler{writer: os.Stdout, tty: isTTY(int(os.Stdout.Fd()))})
	alog.SetLevelFromString(level)
	return alog.Log
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

	// Set key/value context for further logging with the returned logger
	WithField(key string, value interface{}) *alog.Entry

	// Set key/value context for further logging with the returned logger
	WithFields(keyValues alog.Fielder) *alog.Entry

	// Return a logger with the specified error set, to be included in a subsequent normal logging call
	WithError(err error) *alog.Entry

	// Return map fields associated with this logger, if any (i.e. if this logger was returned from WithField[s])
	// If no fields are set, returns nil
	//Fields() map[string]interface{}
}
