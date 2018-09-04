package storage

import (
	"errors"

	"github.com/go-redis/redis"
)

var (
	ErrNilValue = errors.New("Nil value not allowed")
)

type KV interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

// Provide a basic KV scratch pad, for misc feature usage.
// Not optimized so "big" features don't use this.
type redisKV struct {
	store *redisStore
}

func (s *redisStore) Raw() KV {
	return &redisKV{s}
}

func (kv *redisKV) Get(key string) ([]byte, error) {
	value, err := kv.store.rclient.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(value), nil
}

func (kv *redisKV) Set(key string, value []byte) error {
	if value == nil {
		return ErrNilValue
	}
	return kv.store.rclient.Set(key, value, 0).Err()
}
