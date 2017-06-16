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

	err = db.retries.Add(1234567895, "1239712984", []byte("{\"jid\":\"1239712984\"}"))
	assert.NoError(t, err)
	assert.Equal(t, 3, db.retries.Size())

	results, err := db.retries.RemoveBefore(1234567894)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.retries.Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{
		[]byte("{\"jid\":\"1239712983\"}"),
		[]byte("{\"jid\":\"1239712984\"}"),
	}
	assert.Equal(t, values, results)
}
