package storage

import (
	"sync"
	"sync/atomic"

	"github.com/mperham/faktory/util"
	"github.com/mperham/gorocksdb"
)

type rocksQueue struct {
	name  string
	size  int64
	low   int64
	high  int64
	store *rocksStore
	cf    *gorocksdb.ColumnFamilyHandle
	mu    sync.Mutex
}

func (q *rocksQueue) Name() string {
	return q.name
}

func (q *rocksQueue) Page(start int64, count int64, fn func(index int, k, v []byte) error) error {
	index := 0
	upper := upperBound(q.name)

	ro := queueReadOptions(false)
	ro.SetIterateUpperBound(upper)
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := q.store.db.NewIteratorCF(ro, q.cf)
	defer it.Close()

	prefix := append([]byte(q.name), 0xFF)
	it.Seek(prefix)
	if it.Err() != nil {
		return it.Err()
	}

	// skip any before start point
	for i := start; i > 0; i-- {
		if !it.Valid() {
			return nil
		}
		it.Next()
	}

	for ; it.Valid(); it.Next() {
		if count == 0 {
			break
		}
		if err := it.Err(); err != nil {
			return err
		}

		k := it.Key()
		v := it.Value()
		value := v.Data()
		key := k.Data()
		err := fn(index, key, value)
		index += 1
		k.Free()
		v.Free()
		if err != nil {
			return err
		}
		count -= 1
	}
	if it.Err() != nil {
		return it.Err()
	}
	return nil
}

func (q *rocksQueue) Each(fn func(index int, k, v []byte) error) error {
	return q.Page(0, -1, fn)
}

func (q *rocksQueue) Clear() (int, error) {
	count := 0
	// TODO impl which uses Rocks range deletes?
	q.mu.Lock()
	defer q.mu.Unlock()

	upper := upperBound(q.name)
	ro := queueReadOptions(true)
	ro.SetIterateUpperBound(upper)
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := q.store.db.NewIteratorCF(ro, q.cf)
	defer it.Close()

	prefix := append([]byte(q.name), 0xFF)
	it.Seek(prefix)
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
		err := q.store.db.DeleteCF(wo, q.cf, key)
		if err != nil {
			return count, err
		}
		k.Free()
		count += 1
		atomic.AddInt64(&q.low, 1)
		atomic.AddInt64(&q.size, -1)
	}
	return count, nil
}

func (q *rocksQueue) Init() error {
	q.mu = sync.Mutex{}
	upper := upperBound(q.name)

	ro := queueReadOptions(false)
	ro.SetIterateUpperBound(upper)
	ro.SetFillCache(false)
	defer ro.Destroy()

	var count int64
	it := q.store.db.NewIteratorCF(ro, q.cf)
	defer it.Close()

	prefix := append([]byte(q.name), 0xFF)
	it.Seek(prefix)
	if it.Err() != nil {
		return it.Err()
	}

	if it.Valid() {
		k := it.Key()
		key := k.Data()
		start := len(q.name) + 1
		end := start + 8
		q.low = toInt64(key[start:end])
		k.Free()
	}

	for ; it.Valid(); it.Next() {
		count += 1
	}
	it.SeekToLast()

	if it.Err() != nil {
		return it.Err()
	}

	if it.Valid() {
		k := it.Key()
		key := k.Data()
		start := len(q.name) + 1
		end := start + 8
		q.high = toInt64(key[start:end])
		k.Free()
	}
	q.size = count

	util.Log().Debugf("Queue init: %s %d elements %d/%d", q.name, q.size, q.low, q.high)
	return nil
}

func (q *rocksQueue) Size() int64 {
	return q.size
}

func (q *rocksQueue) Push(payload []byte) error {
	k := q.nextkey()
	v := payload
	wo := queueWriteOptions()
	defer wo.Destroy()
	err := q.store.db.PutCF(wo, q.cf, k, v)
	if err != nil {
		return err
	}

	atomic.AddInt64(&q.size, 1)
	return nil
}

func (q *rocksQueue) Pop() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	ro := queueReadOptions(true)
	ro.SetIterateUpperBound(keyfor(q.name, q.low))
	defer ro.Destroy()

	key := keyfor(q.name, q.low)
	value, err := q.store.db.GetBytesCF(ro, q.cf, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	wo := queueWriteOptions()
	defer wo.Destroy()

	err = q.store.db.DeleteCF(wo, q.cf, key)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&q.low, 1)
	atomic.AddInt64(&q.size, -1)
	return value, nil
}

func (q *rocksQueue) nextkey() []byte {
	nxtseq := atomic.AddInt64(&q.high, 1)
	return keyfor(q.name, nxtseq-1)
}

/*
Each entry has a key of the form:
  [queue_name] ["|"] [8 byte seq_id]
We can scan the queue by iterating over the "queue_name" prefix
*/
func keyfor(name string, seq int64) []byte {
	bytes := make([]byte, len(name)+1+8)
	copy(bytes, name)
	len := len(name) + 1
	bytes[len-1] = 0xFF
	bytes[len+0] = byte(seq >> 56)
	bytes[len+1] = byte((seq >> 48) & 0xFF)
	bytes[len+2] = byte((seq >> 40) & 0xFF)
	bytes[len+3] = byte((seq >> 32) & 0xFF)
	bytes[len+4] = byte((seq >> 24) & 0xFF)
	bytes[len+5] = byte((seq >> 16) & 0xFF)
	bytes[len+6] = byte((seq >> 8) & 0xFF)
	bytes[len+7] = byte(seq & 0xFF)
	return bytes
}

func upperBound(name string) []byte {
	bytes := make([]byte, 8+len(name)+1)
	copy(bytes, name)
	len := len(name) + 1
	bytes[len-1] = 0xFF
	bytes[len+0] = 0x7F
	bytes[len+1] = 0xFF
	bytes[len+2] = 0xFF
	bytes[len+3] = 0xFF
	bytes[len+4] = 0xFF
	bytes[len+5] = 0xFF
	bytes[len+6] = 0xFF
	bytes[len+7] = 0xFF
	return bytes
}

func toInt64(bytes []byte) int64 {
	value := int64(bytes[0])
	for i := 1; i < 8; i++ {
		value = (value << 8) + int64(bytes[i])
	}
	return value
}

func queueReadOptions(tailing bool) *gorocksdb.ReadOptions {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetTailing(tailing)
	return ro
}

func queueWriteOptions() *gorocksdb.WriteOptions {
	return gorocksdb.NewDefaultWriteOptions()
}
