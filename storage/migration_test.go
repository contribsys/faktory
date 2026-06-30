package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigrations(t *testing.T) {
	withRedis(t, "migration", func(t *testing.T, store Store) {
		bg := context.Background()

		t.Run("Migrations", func(t *testing.T) {
			_ = store.Flush(bg)
			_, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)

			ver, err := store.DataVersion(bg)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, ver)

			ver, err = store.ApplyMigrations(bg)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, ver)
		})
	})
}
