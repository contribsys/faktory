package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisKV(t *testing.T) {
	withRedis(t, "default", func(t *testing.T, store Store) {
		ctx := context.Background()
		store.Flush(ctx)
		kv := store.Raw()
		assert.NotNil(t, kv)

		val, err := kv.Get(ctx, "mike")
		assert.NoError(t, err)
		assert.Nil(t, val)

		err = kv.Set(ctx, "bob", nil)
		assert.Equal(t, ErrNilValue, err)

		err = kv.Set(ctx, "mike", []byte("bob"))
		assert.NoError(t, err)

		val, err = kv.Get(ctx, "mike")
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
	stopper, err := Boot(dir, sock)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	store, err := Open(sock, 10)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	fn(t, store)
}
