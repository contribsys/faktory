package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Thing struct {
}

func (t Thing) Close() error {
	return nil
}

func TestPool(t *testing.T) {
	pool, err := NewChannelPool(0, 10, func() (Closeable, error) { return Thing{}, nil })
	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Equal(t, 0, Len())

	thng, err := Get()
	assert.NoError(t, err)
	assert.NotNil(t, thng)
	assert.Equal(t, 0, Len())

	Close()
	assert.Equal(t, 1, Len())

	Close()
	assert.Equal(t, 0, Len())
}
