package worq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue("default")
	assert.Equal(t, 0, q.Size())

	j := &Job{}
	assert.Equal(t, nil, q.Push(j))
	assert.Equal(t, 1, q.Size())

	assert.Equal(t, j, q.Pop())
	assert.Equal(t, 0, q.Size())
	j = q.Pop()
	assert.Nil(t, j)
}
