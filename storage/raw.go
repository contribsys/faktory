package storage

import (
	"errors"

	"github.com/contribsys/gorocksdb"
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
type rocksKV struct {
	store *rocksStore
}

func (s *rocksStore) Raw() KV {
	return &rocksKV{s}
}

func (kv *rocksKV) Get(key string) ([]byte, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	value, err := kv.store.db.GetBytesCF(ro, kv.store.defalt, []byte(key))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (kv *rocksKV) Set(key string, value []byte) error {
	if value == nil {
		return ErrNilValue
	}
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err := kv.store.db.PutCF(wo, kv.store.defalt, []byte(key), value)
	if err != nil {
		return err
	}
	return nil
}
