package server

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPasswords(t *testing.T) {
	t.Run("DevWithPassword", func(t *testing.T) {
		opts := &ServerOptions{
			Environment:     "development",
			ConfigDirectory: "../test/auth",
		}

		pwd, err := fetchPassword(opts)
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
	})

	t.Run("DevWithoutPassword", func(t *testing.T) {
		opts := &ServerOptions{
			Environment:     "development",
			ConfigDirectory: "../test/foo",
		}

		pwd, err := fetchPassword(opts)
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	t.Run("ProductionWithoutPassword", func(t *testing.T) {
		opts := &ServerOptions{
			Environment:     "production",
			ConfigDirectory: "../test/foo",
		}

		pwd, err := fetchPassword(opts)
		assert.Error(t, err)
		assert.Equal(t, "", pwd)
	})
	t.Run("ProductionWithPassword", func(t *testing.T) {
		opts := &ServerOptions{
			Environment:     "production",
			ConfigDirectory: "../test/auth",
		}

		pwd, err := fetchPassword(opts)
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
	})
	t.Run("ProductionSkipPassword", func(t *testing.T) {
		os.Setenv("FAKTORY_SKIP_PASSWORD", "yes")

		opts := &ServerOptions{
			Environment:     "production",
			ConfigDirectory: "../test/foo",
		}

		pwd, err := fetchPassword(opts)
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	os.Unsetenv("FAKTORY_SKIP_PASSWORD")
}
