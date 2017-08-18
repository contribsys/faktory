package util

import (
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
