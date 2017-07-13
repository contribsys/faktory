package util

import (
	"log"
	"os"

	"github.com/sirupsen/logrus"
)

// This generic logging interface hide
// a Logrus logger or another impl
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})

	// Log and terminate process (unrecoverable)
	Fatal(args ...interface{})

	// Log with fmt.Printf-like formatting and terminate process (unrecoverable)
	Fatalf(format string, args ...interface{})

	// Log and panic (recoverable)
	Panic(args ...interface{})

	// Log with fmt.Printf-like formatting, then panic (recoverable)
	Panicf(format string, args ...interface{})

	// Set key/value context for further logging with the returned logger
	WithField(key string, value interface{}) Logger

	// Set key/value context for further logging with the returned logger
	WithFields(keyValues map[string]interface{}) Logger

	// Return a logger with the specified error set, to be included in a subsequent normal logging call
	WithError(err error) Logger

	// Return map fields associated with this logger, if any (i.e. if this logger was returned from WithField[s])
	// If no fields are set, returns nil
	//Fields() map[string]interface{}
}

type logrusLoggerOrEntry interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	WithField(key string, value interface{}) *logrus.Entry
	WithFields(keyValues logrus.Fields) *logrus.Entry
	WithError(err error) *logrus.Entry
}

func NewLogger(level string, takeOverLog bool) Logger {
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableSorting:  true,
		FullTimestamp:   true,
		TimestampFormat: TimestampFormat,
	})
	logg := logrus.New()
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		lvl = logrus.WarnLevel
	}
	logg.SetLevel(lvl)

	if takeOverLog {
		log.SetOutput(logg.Writer())
		SetLogLevel(level)
	}
	return newLogrusLogger(logg).WithFields(map[string]interface{}{
		"pid": os.Getpid(),
	})
}

type logrusLogger struct {
	logrusLoggerOrEntry
}

func newLogrusLogger(loggerOrEntry logrusLoggerOrEntry) Logger {
	return logrusLogger{logrusLoggerOrEntry: loggerOrEntry}
}

func (l logrusLogger) WithField(key string, value interface{}) Logger {
	return newLogrusLogger(l.logrusLoggerOrEntry.WithField(key, value))
}

func (l logrusLogger) WithFields(logFields map[string]interface{}) Logger {
	if logFields == nil {
		return l
	}

	return newLogrusLogger(l.logrusLoggerOrEntry.WithFields(logrus.Fields(logFields)))
}

func (l logrusLogger) WithError(err error) Logger {
	return newLogrusLogger(l.logrusLoggerOrEntry.WithError(err))
}

func (l logrusLogger) Fields() map[string]interface{} {
	entry, ok := l.logrusLoggerOrEntry.(*logrus.Entry)
	if ok && entry.Data != nil {
		return entry.Data
	}

	return nil
}
