package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisKV(t *testing.T) {
	db, teardown := setupTest(t)
	defer teardown(t)

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
}
