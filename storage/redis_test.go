package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisKV(t *testing.T) {
	withRedis(t, "default", func(t *testing.T, store Store) {
		store.Flush()
		kv := store.Raw()
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

func withRedis(t *testing.T, name string, fn func(*testing.T, Store)) {
	t.Parallel()

	dir := fmt.Sprintf("/tmp/faktory-test-%s", name)
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/redis.sock", dir)
	stopper, err := BootRedis(dir, sock)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	store, err := OpenRedis(sock, 10)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	fn(t, store)
}
