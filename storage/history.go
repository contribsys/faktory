package storage

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/contribsys/gorocksdb"
)

type processingHistory struct {
	TotalProcessed uint64
	TotalFailures  uint64
}

var (
	ONE          = make([]byte, 8)
	writeOptions = gorocksdb.NewDefaultWriteOptions()
	historyCache = map[string]uint64{}
)

func init() {
	binary.PutUvarint(ONE, 1)
}

func (store *rocksStore) Success() error {
	atomic.AddUint64(&store.history.TotalProcessed, 1)
	daystr := time.Now().Format("2006-01-02")
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Processed:%s", daystr)), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte("Processed"), ONE)
	return nil
}

func (store *rocksStore) Failure() error {
	atomic.AddUint64(&store.history.TotalProcessed, 1)
	atomic.AddUint64(&store.history.TotalFailures, 1)
	store.db.MergeCF(writeOptions, store.stats, []byte("Processed"), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte("Failures"), ONE)

	daystr := time.Now().Format("2006-01-02")
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Processed:%s", daystr)), ONE)
	store.db.MergeCF(writeOptions, store.stats, []byte(fmt.Sprintf("Failures:%s", daystr)), ONE)
	return nil
}

func cachedStat(store *rocksStore, name string) (uint64, error) {
	val, ok := historyCache[name]
	if ok {
		return val, nil
	}

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	value, err := store.db.GetBytesCF(ro, store.stats, []byte(name))
	if err != nil {
		return 0, err
	}
	if value != nil {
		proc, _ := binary.Uvarint(value)
		historyCache[name] = proc
		return proc, nil
	}
	historyCache[name] = 0
	return 0, nil
}

func (store *rocksStore) History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	ts := time.Now()
	daystr := ts.Format("2006-01-02")

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	var proc uint64
	var failed uint64
	// today's data we always get fresh from RocksDB
	value, err := store.db.GetBytesCF(ro, store.stats, []byte(fmt.Sprintf("Processed:%s", daystr)))
	if err != nil {
		return err
	}
	if value != nil {
		proc, _ = binary.Uvarint(value)
	}
	value, err = store.db.GetBytesCF(ro, store.stats, []byte(fmt.Sprintf("Failures:%s", daystr)))
	if err != nil {
		return err
	}
	if value != nil {
		failed, _ = binary.Uvarint(value)
	}
	fn(daystr, proc, failed)
	proc = 0
	failed = 0

	// we can cache previous day's values
	for i := 1; i < days; i++ {
		ts = ts.Add(-24 * time.Hour)
		daystr = ts.Format("2006-01-02")
		proc, err = cachedStat(store, fmt.Sprintf("Processed:%s", daystr))
		if err != nil {
			return err
		}
		failed, err = cachedStat(store, fmt.Sprintf("Failures:%s", daystr))
		if err != nil {
			return err
		}
		fn(daystr, proc, failed)
		proc = 0
		failed = 0
	}
	return nil
}

///////////////////////////

type uint64CounterMerge struct {
}

func (m *uint64CounterMerge) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var eint uint64
	if existingValue != nil {
		eint, _ = binary.Uvarint(existingValue)
	}

	for _, val := range operands {
		oint, _ := binary.Uvarint(val)
		eint += oint
	}

	newval := make([]byte, 8)
	binary.PutUvarint(newval, eint)
	return newval, true
}

func (m *uint64CounterMerge) PartialMerge(key, left, right []byte) ([]byte, bool) {
	aint, _ := binary.Uvarint(left)
	bint, _ := binary.Uvarint(right)

	data := make([]byte, 8)
	binary.PutUvarint(data, aint+bint)
	return data, true
}

func (m *uint64CounterMerge) Name() string {
	return "faktory stats counters"
}
