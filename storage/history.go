package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
)

type processingHistory struct {
	TotalProcessed int64
	TotalFailures  int64
}

var (
	statsMu = sync.Mutex{}
	prefix  = "z~"
)

func (store *bStore) Incr(name string) error {
	statsMu.Lock()
	defer statsMu.Unlock()

	return store.bdb.Update(func(tx *badger.Txn) error {
		key := prefix + name
		item, err := tx.Get([]byte(key))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		var before int64
		if err == nil {
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			before, _ = binary.Varint(value)
		}
		newval := make([]byte, 8)
		binary.PutVarint(newval, before+1)
		return tx.Set([]byte(key), newval)
	})
	return nil
}

func (store *bStore) Success() error {
	atomic.AddInt64(&store.history.TotalProcessed, 1)
	store.Incr(fmt.Sprintf("Processed:%s", time.Now().Format("2006-01-02")))
	store.Incr("Processed")
	return nil
}

func (store *bStore) Failure() error {
	atomic.AddInt64(&store.history.TotalProcessed, 1)
	atomic.AddInt64(&store.history.TotalFailures, 1)
	store.Incr("Processed")
	store.Incr("Failures")
	daystr := time.Now().Format("2006-01-02")
	store.Incr(fmt.Sprintf("Processed:%s", daystr))
	store.Incr(fmt.Sprintf("Failures:%s", daystr))
	return nil
}

func (store *bStore) Failures() int64 {
	return store.history.TotalFailures
}

func (store *bStore) Processed() int64 {
	return store.history.TotalProcessed
}

func (store *bStore) History(days int, fn func(day string, procCnt int64, failCnt int64)) error {
	ts := time.Now()

	var proc int64
	var failed int64
	return store.bdb.View(func(tx *badger.Txn) error {
		for i := 0; i < days; i++ {
			daystr := ts.Format("2006-01-02")
			value, err := tx.Get([]byte(fmt.Sprintf("%sProcessed:%s", prefix, daystr)))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if value != nil {
				val, err := value.ValueCopy(nil)
				if err != nil {
					return err
				}
				proc, _ = binary.Varint(val)
			}
			value, err = tx.Get([]byte(fmt.Sprintf("%sFailures:%s", prefix, daystr)))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if value != nil {
				val, err := value.ValueCopy(nil)
				if err != nil {
					return err
				}
				failed, _ = binary.Varint(val)
			}
			fn(daystr, proc, failed)
			proc = 0
			failed = 0
			ts = ts.Add(-24 * time.Hour)
		}
		return nil
	})
}
