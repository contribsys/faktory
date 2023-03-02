package storage

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNilValue = errors.New("Nil value not allowed")
)

type KV interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
}

// Provide a basic KV scratch pad, for misc feature usage.
// Not optimized so "big" features don't use this.
type redisKV struct {
	store *redisStore
}

func (s *redisStore) Raw() KV {
	return &redisKV{s}
}

func (kv *redisKV) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := kv.store.rclient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(value), nil
}

func (kv *redisKV) Set(ctx context.Context, key string, value []byte) error {
	if value == nil {
		return ErrNilValue
	}
	return kv.store.rclient.Set(ctx, key, value, 0).Err()
}
