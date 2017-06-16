package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicTimedSet(t *testing.T) {
	t.Parallel()

	db, err := OpenStore("../test/timed.db")
	assert.NoError(t, err)

	assert.Equal(t, 0, db.retries.Size())
	err = db.retries.Add(1234567890, "1239712983", []byte("{\"jid\":\"1239712983\"}"))
	assert.NoError(t, err)
	assert.Equal(t, 1, db.retries.Size())

	err = db.retries.Add(1234567890, "1239712984", []byte("{\"jid\":\"1239712984\"}"))
	assert.NoError(t, err)
	assert.Equal(t, 2, db.retries.Size())
}
