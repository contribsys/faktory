package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	t.Parallel()

	t.Run("KV", func(t *testing.T) {
		t.Parallel()

		db, err := OpenRedis()
		assert.NoError(t, err)
		defer db.Close()

		kv := db.Raw()
		assert.NotNil(t, kv)

		val, err := kv.Get("mike")
		assert.NoError(t, err)
		assert.Nil(t, val)

		err = kv.Set("bob", nil)
		assert.Equal(t, ErrNilValue, err)

		err = kv.Set("mike", []byte("bob"))
		assert.NoError(t, err)

		val, err = kv.Get("mike")
		assert.NoError(t, err)
		assert.NotNil(t, val)
		assert.Equal(t, "bob", string(val))

	})
}
