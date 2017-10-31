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
	none   = 0
	red    = 31
	green  = 32
	yellow = 33
	blue   = 34
	gray   = 37
)

// Colors mapping.
var Colors = [...]int{
	alog.DebugLevel: gray,
	alog.InfoLevel:  blue,
	alog.WarnLevel:  yellow,
	alog.ErrorLevel: red,
	alog.FatalLevel: red,
}

// Strings mapping.
var Strings = [...]string{
	alog.DebugLevel: "DEBUG",
	alog.InfoLevel:  "INFO",
	alog.WarnLevel:  "WARN",
	alog.ErrorLevel: "ERROR",
	alog.FatalLevel: "FATAL",
}

type LogHandler struct {
	mu     sync.Mutex
	Writer io.Writer
}

func (h *LogHandler) HandleLog(e *alog.Entry) error {
	color := Colors[e.Level]
	level := Strings[e.Level]
	names := e.Fields.Names()
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Fprintf(h.Writer, "\033[%dm%6s\033[0m %s %-25s", color, level, ts, e.Message)

	for _, name := range names {
		fmt.Fprintf(h.Writer, " \033[%dm%s\033[0m=%v", color, name, e.Fields.Get(name))
	}

	fmt.Fprintln(h.Writer)

	return nil
}

func NewLogger(level string, takeOverLog bool) Logger {

	alog.SetHandler(&LogHandler{Writer: os.Stdout})
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
