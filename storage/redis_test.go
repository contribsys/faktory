package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisStartup(t *testing.T) {
	r, err := OpenRedis("/tmp/redis")
	assert.NoError(t, err)
	assert.NotNil(t, r)

	err = r.Close()
	assert.NoError(t, err)
}
