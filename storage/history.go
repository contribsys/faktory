package storage

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mperham/gorocksdb"
)

type processingHistory struct {
	TotalProcessed int64
	TotalFailures  int64
}

var (
	ONE          = make([]byte, 8)
	writeOptions = gorocksdb.NewDefaultWriteOptions()
)

func init() {
	binary.PutVarint(ONE, 1)
}

func (store *rocksStore) Success() error {
	atomic.AddInt64(&store.history.TotalProcessed, 1)
	daystr := time.Now().Format("2006-01-05")
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Processed:%s", daystr)), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte("Processed"), ONE)
	return nil
}

func (store *rocksStore) Failure() error {
	atomic.AddInt64(&store.history.TotalProcessed, 1)
	atomic.AddInt64(&store.history.TotalFailures, 1)

	daystr := time.Now().Format("2006-01-05")
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Processed:%s", daystr)), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Failures:%s", daystr)), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte("Processed"), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte("Failures"), ONE)
	return nil
}

type Int64CounterMerge struct {
}

func (m *Int64CounterMerge) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	eint, _ := binary.Varint(existingValue)
	for _, val := range operands {
		oint, _ := binary.Varint(val)
		eint += oint
	}

	newval := make([]byte, 8)
	binary.PutVarint(newval, eint)
	return newval, true
}

func (m *Int64CounterMerge) PartialMerge(key, left, right []byte) ([]byte, bool) {
	aint, _ := binary.Varint(left)
	bint, _ := binary.Varint(right)

	data := make([]byte, 8)
	binary.PutVarint(data, aint+bint)
	return data, true
}

func (m *Int64CounterMerge) Name() string {
	return "faktory counter merge"
}
