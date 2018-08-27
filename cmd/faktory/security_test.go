package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPasswords(t *testing.T) {
	t.Run("DevWithPassword", func(t *testing.T) {
		pwd, err := fetchPassword("../../test/auth", "development")
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
	})

	t.Run("DevWithoutPassword", func(t *testing.T) {
		pwd, err := fetchPassword("../../test/foo", "development")
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	t.Run("ProductionWithoutPassword", func(t *testing.T) {
		pwd, err := fetchPassword("../../test/foo", "production")
		assert.Error(t, err)
		assert.Equal(t, "", pwd)
	})

	t.Run("ProductionWithPassword", func(t *testing.T) {
		pwd, err := fetchPassword("../../test/auth", "production")
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
	})

	t.Run("ProductionSkipPassword", func(t *testing.T) {
		os.Setenv("FAKTORY_SKIP_PASSWORD", "yes")

		pwd, err := fetchPassword("../../test/foo", "production")
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	os.Unsetenv("FAKTORY_SKIP_PASSWORD")
}
