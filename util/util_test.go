package util

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseTime(t *testing.T) {
	// Ruby's iso8601 format:
	// require 'time'
	// Time.now.utc.iso8601
	tm, err := ParseTime("2017-08-17T18:55:26Z")
	assert.Nil(t, err)
	assert.True(t, tm.Before(time.Now()))

	tm, err = ParseTime("2017-08-17T18:55:26.554544Z")
	assert.Nil(t, err)
	assert.True(t, tm.Before(time.Now()))

	now := time.Now().UTC()
	then, err := ParseTime(Thens(now))
	assert.Nil(t, err)
	assert.Equal(t, now, then)
}

func TestBacktrace(t *testing.T) {
	ex := Backtrace(12)
	assert.NotNil(t, ex)
	assert.True(t, len(ex) > 2)
	assert.True(t, len(ex) < 12)
	assert.Contains(t, ex[0], "TestBacktrace")

	LogInfo = true
	DumpProcessTrace()
}

func TestLogger(t *testing.T) {
	InitLogger("debug")
	Debug("hello")
	Debugf("hello %s", "mike")
	Info("hello")
	Infof("hello %s", "mike")
	Warn("hello")
	Warnf("hello %s", "mike")
	Error("hello", os.ErrClosed)
}

func TestMisc(t *testing.T) {
	Darwin()

	ok, err := FileExists("./nope.go")
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = FileExists("./util_test.go")
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Equal(t, 16, len(RandomJid()))

	ts := Nows()
	assert.True(t, strings.HasPrefix(ts, "20"))

	val := MemoryUsageMB()
	assert.True(t, val < 100)
}
