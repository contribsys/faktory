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
	assert.Equal(t, 0, pool.Len())

	thng, err := pool.Get()
	assert.NoError(t, err)
	assert.NotNil(t, thng)
	assert.Equal(t, 0, pool.Len())

	thng.Close()
	assert.Equal(t, 1, pool.Len())

	pool.Close()
	assert.Equal(t, 0, pool.Len())
}
