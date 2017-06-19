package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicQueue(t *testing.T) {
	t.Parallel()

	db, err := OpenStore("../test/basic.db")
	assert.NoError(t, err)

	q := db.NewQueue("default")
	assert.Equal(t, 0, q.Size())

	j := &[]byte{}
	assert.Equal(t, nil, q.Push(j))
	assert.Equal(t, 1, q.Size())

	assert.Equal(t, j, q.Pop())
	assert.Equal(t, 0, q.Size())
	x := q.Pop()
	assert.Nil(t, x)
}
