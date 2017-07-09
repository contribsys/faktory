package storage

import (
	"sync/atomic"

	"github.com/mperham/gorocksdb"
	"github.com/mperham/worq/util"
)

var (
	MaxInt = int64(^uint64(0) >> 1)
)

type RocksQueue struct {
	Name  string
	size  int64
	low   int64
	high  int64
	store *RocksStore
	cf    *gorocksdb.ColumnFamilyHandle
}

func (q *RocksQueue) Init() error {
	upper := upperBound(q.Name)

	ro := queueReadOptions(false)
	ro.SetIterateUpperBound(upper)
	ro.SetFillCache(false)
	defer ro.Destroy()

	var count int64
	it := q.store.db.NewIteratorCF(ro, q.cf)
	defer it.Close()

	prefix := append([]byte(q.Name), 0xFF)
	it.Seek(prefix)
	if it.Err() != nil {
		return it.Err()
	}

	if it.Valid() {
		k := it.Key()
		key := k.Data()
		start := len(q.Name) + 1
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
		start := len(q.Name) + 1
		end := start + 8
		q.high = toInt64(key[start:end])
		k.Free()
	}
	q.size = count

	util.Debug("Queue init: %s %d elements %d/%d\n", q.Name, q.size, q.low, q.high)
	return nil
}

func (q *RocksQueue) Size() int64 {
	return q.size
}

func (q *RocksQueue) Push(payload []byte) error {
	k := q.nextkey()
	v := payload
	atomic.AddInt64(&q.size, 1)
	wo := queueWriteOptions()
	defer wo.Destroy()
	return q.store.db.PutCF(wo, q.cf, k, v)
}

func (q *RocksQueue) Pop() ([]byte, error) {
	ro := queueReadOptions(true)
	defer ro.Destroy()

	key := keyfor(q.Name, q.low)
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

func (q *RocksQueue) nextkey() []byte {
	nxtseq := atomic.AddInt64(&q.high, 1)
	return keyfor(q.Name, nxtseq-1)
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
