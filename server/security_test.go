package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPasswords(t *testing.T) {
	pwd, err := fetchPassword("../test/auth")
	assert.NoError(t, err)
	assert.Equal(t, 16, len(pwd))

	pwd, err = fetchPassword("../test/foo")
	assert.NoError(t, err)
	assert.Equal(t, "", pwd)
}
