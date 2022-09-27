package cli

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func pwdCfg(value string) map[string]interface{} {
	return map[string]interface{}{
		"faktory": map[string]interface{}{
			"password": value,
		},
	}
}

func TestPasswords(t *testing.T) {
	emptyCfg := map[string]interface{}{}
	// nolint:gosec
	pwd := "cce29d6565ab7376"

	t.Run("DevWithPassword", func(t *testing.T) {
		cfg := pwdCfg(pwd)
		pwd, err := fetchPassword(cfg, "development")
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
		assert.Equal(t, "********", cfg["faktory"].(map[string]interface{})["password"])
	})

	t.Run("DevWithoutPassword", func(t *testing.T) {
		pwd, err := fetchPassword(emptyCfg, "development")
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	t.Run("ProductionWithoutPassword", func(t *testing.T) {
		pwd, err := fetchPassword(emptyCfg, "production")
		assert.Error(t, err)
		assert.Equal(t, "", pwd)
	})

	t.Run("ProductionWithFile", func(t *testing.T) {
		// nolint:gosec
		err := os.WriteFile("/tmp/test-password", []byte("foobar"), os.FileMode(0o666))
		assert.NoError(t, err)
		cfg := pwdCfg("/tmp/test-password")
		pwd, err := fetchPassword(cfg, "production")
		assert.NoError(t, err)
		assert.Equal(t, "foobar", pwd)
	})

	t.Run("ProductionWithPassword", func(t *testing.T) {
		cfg := pwdCfg(pwd)
		pwd, err := fetchPassword(cfg, "production")
		assert.NoError(t, err)
		assert.Equal(t, 16, len(pwd))
		assert.Equal(t, "cce29d6565ab7376", pwd)
		assert.Equal(t, "********", cfg["faktory"].(map[string]interface{})["password"])
	})

	t.Run("ProductionEnvPassword", func(t *testing.T) {
		os.Setenv("FAKTORY_PASSWORD", "abc123")

		pwd, err := fetchPassword(emptyCfg, "production")
		assert.NoError(t, err)
		assert.Equal(t, "abc123", pwd)
	})

	os.Unsetenv("FAKTORY_PASSWORD")

	t.Run("ProductionSkipPassword", func(t *testing.T) {
		os.Setenv("FAKTORY_SKIP_PASSWORD", "yes")

		pwd, err := fetchPassword(emptyCfg, "production")
		assert.NoError(t, err)
		assert.Equal(t, "", pwd)
	})

	os.Unsetenv("FAKTORY_SKIP_PASSWORD")
}
