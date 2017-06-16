package cli

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCli(t *testing.T) {
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	assert.NoError(t, err)
	SetupLogging(file)
	help()
}
