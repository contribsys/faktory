package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/contribsys/gorocksdb"
)

// Retries and Scheduled jobs are held in a bucket, sorted based on their timestamp.
type rocksSortedSet struct {
	name string
	db   *gorocksdb.DB
	cf   *gorocksdb.ColumnFamilyHandle
	ro   *gorocksdb.ReadOptions
	wo   *gorocksdb.WriteOptions

	// this is a cache of the number of elements within the ColumnFamily.
	// unfortunately it is possible for this number to get out of sync so we need
	// to monitor its value and reset it occasionally.
	size uint64
}

func (ts *rocksSortedSet) Name() string {
	return ts.name
}

func (ts *rocksSortedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))
	err := ts.db.PutCF(ts.wo, ts.cf, key, payload)
	if err != nil {
		return err
	}
	atomic.AddUint64(&ts.size, 1)
	return nil
}

func (ts *rocksSortedSet) Close() {
	ts.cf.Destroy()
}

func (ts *rocksSortedSet) Page(start int64, count int64, proc func(int, []byte, []byte) error) error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := ts.db.NewIteratorCF(ro, ts.cf)
	defer it.Close()

	it.SeekToFirst()
	// skip any before start point
	for i := start; i > 0; i-- {
		if !it.Valid() {
			return nil
		}
		it.Next()
	}

	index := 0
	for ; it.Valid(); it.Next() {
		if count == 0 {
			break
		}
		if err := it.Err(); err != nil {
			return err
		}
		k := it.Key()
		v := it.Value()
		key := k.Data()
		payload := v.Data()
		err := proc(index, key, payload)
		index++
		k.Free()
		v.Free()
		if err != nil {
			return err
		}
		count -= 1
	}

	return nil
}

func (ts *rocksSortedSet) Each(proc func(int, []byte, []byte) error) error {
	return ts.Page(0, -1, proc)
}

func (ts *rocksSortedSet) Get(key []byte) ([]byte, error) {
	data, err := ts.db.GetBytesCF(ts.ro, ts.cf, key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (ts *rocksSortedSet) init() *rocksSortedSet {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := ts.db.NewIteratorCF(ro, ts.cf)
	defer it.Close()

	var count uint64
	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}
	if err := it.Err(); err != nil {
		panic(fmt.Sprintf("%s size: %s", ts.name, err.Error()))
	}
	atomic.StoreUint64(&ts.size, count)
	return ts
}

const (
	MAX uint64 = 2<<63 - 1
)

func (ts *rocksSortedSet) Reset() int {
	sz := atomic.LoadUint64(&ts.size)
	if sz > MAX {
		// The size has "gone negative" as a uint.
		// Reset it by iterating through the data.
		ts.init()
		return 1
	}
	return 0
}

func (ts *rocksSortedSet) Size() uint64 {
	return atomic.LoadUint64(&ts.size)
}

func (ts *rocksSortedSet) Remove(key []byte) error {
	d, err := ts.Get(key)
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}
	err = ts.db.DeleteCF(ts.wo, ts.cf, key)
	if err != nil {
		return err
	}
	atomic.AddUint64(&ts.size, ^uint64(0))
	return nil
}

func (ts *rocksSortedSet) RemoveElement(tstamp string, jid string) error {
	return ts.Remove([]byte(fmt.Sprintf("%s|%s", tstamp, jid)))
}

func (ts *rocksSortedSet) RemoveBefore(tstamp string) ([][]byte, error) {
	prefix := []byte(tstamp + "|")
	results := [][]byte{}
	count := uint64(0)

	// TODO does Rocks have range deletes?
	wb := gorocksdb.NewWriteBatch()
	it := ts.db.NewIteratorCF(ts.ro, ts.cf)
	defer it.Close()
	it.SeekForPrev(prefix)
	for ; it.Valid(); it.Prev() {
		k := it.Key()
		v := it.Value()
		cp := make([]byte, v.Size())
		copy(cp, v.Data())
		results = append(results, cp)
		wb.DeleteCF(ts.cf, k.Data())
		k.Free()
		v.Free()
		count++
	}
	if count > 0 {
		err := ts.db.Write(ts.wo, wb)
		if err != nil {
			return nil, err
		}
		atomic.AddUint64(&ts.size, ^uint64(count-1))
	}

	// reverse results since we iterated backwards
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	return results, nil
}

func (ts *rocksSortedSet) MoveTo(ots SortedSet, tstamp string, jid string, mutator func(value []byte) (string, []byte, error)) error {
	other := ots.(*rocksSortedSet)
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	slice, err := ts.db.GetCF(ts.ro, ts.cf, key)
	if err != nil {
		return err
	}
	defer slice.Free()

	data := slice.Data()
	if len(data) == 0 {
		return fmt.Errorf("Element not found in %s: %s", ts.name, jid)
	}

	newtstamp, payload, err := mutator(data)
	if err != nil {
		return err
	}
	newkey := []byte(fmt.Sprintf("%s|%s", newtstamp, jid))

	wb := gorocksdb.NewWriteBatch()
	wb.DeleteCF(ts.cf, key)
	wb.PutCF(other.cf, newkey, payload)
	err = ts.db.Write(ts.wo, wb)
	if err != nil {
		return err
	}
	atomic.AddUint64(&ts.size, ^uint64(0))
	atomic.AddUint64(&other.size, 1)
	return nil
}

func (ts *rocksSortedSet) Clear() (uint64, error) {
	count := uint64(0)

	ro := queueReadOptions(true)
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := ts.db.NewIteratorCF(ts.ro, ts.cf)
	defer it.Close()

	it.SeekToFirst()
	if it.Err() != nil {
		return 0, it.Err()
	}

	if !it.Valid() {
		return 0, nil
	}

	wo := queueWriteOptions()
	defer wo.Destroy()

	for ; it.Valid(); it.Next() {
		k := it.Key()
		key := k.Data()
		err := ts.db.DeleteCF(wo, ts.cf, key)
		if err != nil {
			return count, err
		}
		k.Free()
		count++
	}
	atomic.StoreUint64(&ts.size, 0)
	return count, nil
}
