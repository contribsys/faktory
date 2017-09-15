package storage

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory/util"
	"github.com/mperham/gorocksdb"
)

var (
	/*
	 * The default maximum size of a queue.
	 * Further Pushes will result in an error.
	 *
	 * This is known as "back pressue" and is important to
	 * prevent bugs in one component from taking down the
	 * entire system.
	 */
	DefaultMaxSize = int64(100000)
)

type Backpressure struct {
	QueueName   string
	CurrentSize int64
	MaxSize     int64
}

func (bp Backpressure) Error() string {
	return fmt.Sprintf("%s is too large, currently %d, max size is %d", bp.QueueName, bp.CurrentSize, bp.MaxSize)
}

type rocksQueue struct {
	name    string
	size    int64
	low     int64
	high    int64
	store   *rocksStore
	cf      *gorocksdb.ColumnFamilyHandle
	mu      sync.Mutex
	maxsz   int64
	waiters *list.List
	waitmu  sync.Mutex
	done    bool
}

func (q *rocksQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.clearWaiters()
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

func (q *rocksQueue) Clear() (int64, error) {
	count := int64(0)
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

	wb := gorocksdb.NewWriteBatch()

	for ; it.Valid(); it.Next() {
		k := it.Key()
		key := k.Data()
		//util.Warnf("Queue#clear: delete %x", key)
		wb.DeleteCF(q.cf, key)
		k.Free()
		count += 1
	}
	err := q.store.db.Write(wo, wb)
	if err != nil {
		return count, err
	}
	atomic.AddInt64(&q.low, count)
	atomic.AddInt64(&q.size, -count)
	//util.Warnf("Queue#clear: deleted %d elements from %s, size %d", count, q.name, q.size)
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
		//util.Warnf("Queue#init: first %x", key)
		start := len(q.name) + 1
		end := start + 8
		q.low = toInt64(key[start:end])
		k.Free()
	}

	for ; it.Valid(); it.Next() {
		//k := it.Key()
		//key := k.Data()
		//util.Warnf("Queue#init: element %x", key)
		//k.Free()
		count += 1
	}
	it.SeekToLast()

	if it.Err() != nil {
		return it.Err()
	}

	if it.ValidForPrefix(prefix) {
		k := it.Key()
		key := k.Data()
		//util.Warnf("Queue#init: %s last %x", q.name, key)
		start := len(q.name) + 1
		end := start + 8
		q.high = toInt64(key[start:end]) + 1
		k.Free()
	}
	q.size = count
	q.waiters = list.New()

	util.Debugf("Queue init: %s %d elements, %d/%d", q.name, q.size, q.low, q.high)
	return nil
}

func (q *rocksQueue) Size() int64 {
	return q.size
}

func (q *rocksQueue) Push(payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size > q.maxsz {
		return Backpressure{q.name, q.size, q.maxsz}
	}

	k := q.nextkey()
	v := payload
	wo := queueWriteOptions()
	defer wo.Destroy()
	err := q.store.db.PutCF(wo, q.cf, k, v)
	if err != nil {
		return err
	}

	//util.Warnf("Adding element %x to %s", k, q.name)
	atomic.AddInt64(&q.size, 1)

	q.notify()
	return nil
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *rocksQueue) Pop() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 || q.done {
		return nil, nil
	}

	return q._pop()
}

// caller must hold q.mu or else DOOM!
func (q *rocksQueue) _pop() ([]byte, error) {
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

type QueueWaiter struct {
	ctx      context.Context
	notifier chan bool
}

/**
 * Iterates through our current list of waiters,
 * finds one whose deadline has not passed and signals
 * the waiter via its channel.
 *
 * This is best effort: it's possible for the waiter's
 * deadline to pass just as it is selected to wake up.
 */
func (q *rocksQueue) notify() {
	q.waitmu.Lock()
	defer q.waitmu.Unlock()

	// for loop required to drain old, expired waiters
	for e := q.waiters.Front(); e != nil; e = e.Next() {
		qw := e.Value.(*QueueWaiter)
		q.waiters.Remove(e)

		deadline, _ := qw.ctx.Deadline()
		if time.Now().Before(deadline) {
			qw.notifier <- true
			break
		}
	}
}

func (q *rocksQueue) clearWaiters() {
	q.waitmu.Lock()
	defer q.waitmu.Unlock()

	for e := q.waiters.Front(); e != nil; e = e.Next() {
		qw := e.Value.(*QueueWaiter)
		qw.notifier <- false
	}
	q.waiters = list.New()
}

/*
 * Waits for a job to come onto the queue.  The given context
 * should timeout the blocking.
 *
 * Waiter calls this method, blocks on a channel it creates.
 * Sends that channel to the dispatcher via waiter channel.
 * Dispatcher is notified of new job on queue, pulls off a waiter
 * and pushes notification to its channel.
 */
func (q *rocksQueue) BPop(ctx context.Context) ([]byte, error) {
	for {
		q.mu.Lock()
		if q.done {
			q.mu.Unlock()
			return nil, nil
		}
		if q.size > 0 {
			defer q.mu.Unlock()
			return q._pop()
		}
		q.mu.Unlock()

		waiting := make(chan bool, 1)
		me := &QueueWaiter{
			ctx:      ctx,
			notifier: waiting,
		}
		q.waitmu.Lock()
		e := q.waiters.PushBack(me)
		q.waitmu.Unlock()

		select {
		case newjob := <-waiting:
			if newjob {
				continue
			}
			break
		case <-ctx.Done():
			q.waitmu.Lock()
			q.waiters.Remove(e)
			q.waitmu.Unlock()
			return nil, nil
		}
	}
	return nil, nil
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
