package storage

import (
	"fmt"
	"os"
	"testing"
)

func setupTest(t *testing.T) (Store, func(t *testing.T)) {
	store, err := OpenRedis()
	if err != nil {
		panic(err)
	}
	fmt.Println("Flushing redis")
	store.Flush()
	return store, func(t *testing.T) {
		store.Close()
	}
}

func init() {
	os.Setenv("FAKTORY_REDIS_SOCK", "/tmp/faktory-redis-test.sock")
	os.Setenv("FAKTORY_REDIS_PATH", "/tmp/faktory-redis-test")
	MustBootRedis()
}
