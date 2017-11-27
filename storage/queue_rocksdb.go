package storage

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory/storage/brodal"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

var (
	// The default maximum size of a queue.
	// Further Pushes will result in an error.
	//
	// This is known as "back pressue" and is important to
	// prevent bugs in one component from taking down the
	// entire system.
	DefaultMaxSize = uint64(1000000)
)

const (
	NEW_JOB = iota
	CLOSE
)

type Backpressure struct {
	QueueName   string
	CurrentSize uint64
	MaxSize     uint64
}

func (bp Backpressure) Error() string {
	return fmt.Sprintf("%s is too large, currently %d, max size is %d", bp.QueueName, bp.CurrentSize, bp.MaxSize)
}

type queuePointer struct {
	high     uint64
	low      uint64
	priority uint8
}

func (p *queuePointer) Value() int {
	// brodal queues use the lowest priority
	return int(-p.priority)
}

type rocksQueue struct {
	name    string
	size    uint64
	store   *rocksStore
	cf      *gorocksdb.ColumnFamilyHandle
	mu      sync.Mutex
	maxsz   uint64
	waiters *list.List
	waitmu  sync.RWMutex
	done    bool

	orderedPointers *brodal.Heap
	pointers        map[uint8]*queuePointer
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
		index++
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

func (q *rocksQueue) Clear() (uint64, error) {
	count := uint64(0)
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
	defer wb.Destroy()

	for ; it.Valid(); it.Next() {
		k := it.Key()
		key := k.Data()
		//util.Warnf("Queue#clear: delete %x", key)
		wb.DeleteCF(q.cf, key)
		k.Free()
		count++
	}
	err := q.store.db.Write(wo, wb)
	if err != nil {
		return count, err
	}

	// reset the pointer references since we iterated over all keys
	q.orderedPointers = brodal.NewHeap()
	q.pointers = make(map[uint8]*queuePointer)
	atomic.StoreUint64(&q.size, 0)

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

	var count uint64
	it := q.store.db.NewIteratorCF(ro, q.cf)
	defer it.Close()

	prefix := append([]byte(q.name), 0xFF)

	it.Seek(prefix)
	if err := it.Err(); err != nil {
		return err
	}

	for ; it.Valid(); it.Next() {
		k := it.Key()
		key := k.Data()

		_, priority, seq := decodeKey(q.name, key)
		if p, ok := q.pointers[priority]; ok {
			if seq > p.high {
				p.high = seq
			} else if seq < p.low {
				p.low = seq
			}
		} else {
			p = &queuePointer{
				priority: priority,
				high:     seq,
				low:      seq,
			}
			q.orderedPointers.Insert(p)
			q.pointers[priority] = p
		}

		k.Free()
		count++
	}
	it.SeekToLast()

	if err := it.Err(); err != nil {
		return err
	}

	atomic.StoreUint64(&q.size, count)
	q.waiters = list.New()

	util.Debugf("Queue init: %s %d elements", q.name, atomic.LoadUint64(&q.size))
	return nil
}

func (q *rocksQueue) Size() uint64 {
	return atomic.LoadUint64(&q.size)
}

func (q *rocksQueue) Push(priority uint8, payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if size := atomic.LoadUint64(&q.size); size > q.maxsz {
		return Backpressure{q.name, size, q.maxsz}
	}

	k := q.nextkey(priority)
	v := payload
	wo := queueWriteOptions()
	defer wo.Destroy()
	err := q.store.db.PutCF(wo, q.cf, k, v)
	if err != nil {
		return err
	}

	//util.Warnf("Adding element %x to %s", k, q.name)
	atomic.AddUint64(&q.size, 1)

	q.notify()
	return nil
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *rocksQueue) Pop() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if atomic.LoadUint64(&q.size) == 0 || q.done {
		return nil, nil
	}

	return q._pop()
}

// caller must hold q.mu or else DOOM!
func (q *rocksQueue) _pop() ([]byte, error) {
	var value []byte
	var err error

	// got nothing
	if len(q.pointers) == 0 {
		return nil, nil
	}

	p := q.orderedPointers.Peek().(*queuePointer)
	key := makeKey(q.name, p.priority, p.high)
	ro := queueReadOptions(true)
	ro.SetIterateUpperBound(key)
	defer ro.Destroy()

	for {
		key = makeKey(q.name, p.priority, p.low)
		value, err = q.store.db.GetBytesCF(ro, q.cf, key)
		if err != nil {
			return nil, err
		}
		if value != nil {
			break
		}
		if p.low < p.high {
			// If we delete an element from the queue without processing it,
			// a "hole" appears in our counting.  We need to iterate past the
			// hole to find the next valid key.
			atomic.AddUint64(&p.low, 1)
			continue
		}
		// we've iterated past all the "holes" and this pointer is no longer valid
		q.orderedPointers.Pop()
		delete(q.pointers, p.priority)

		return nil, nil
	}

	wo := queueWriteOptions()
	defer wo.Destroy()

	err = q.store.db.DeleteCF(wo, q.cf, key)
	if err != nil {
		return nil, err
	}

	atomic.AddUint64(&p.low, 1)
	if p.low > p.high {
		q.orderedPointers.Pop()
		delete(q.pointers, p.priority)
	}

	// decrement
	atomic.AddUint64(&q.size, ^uint64(0))
	return value, nil
}

type QueueWaiter struct {
	ctx      context.Context
	notifier chan int
}

// Iterates through our current list of waiters,
// finds one whose deadline has not passed and signals
// the waiter via its channel.
//
// This is best effort: it's possible for the waiter's
// deadline to pass just as it is selected to wake up.
func (q *rocksQueue) notify() {
	q.waitmu.Lock()
	defer q.waitmu.Unlock()

	// for loop required to drain old, expired waiters
	for e := q.waiters.Front(); e != nil; e = e.Next() {
		qw := e.Value.(*QueueWaiter)
		q.waiters.Remove(e)

		deadline, _ := qw.ctx.Deadline()
		if time.Now().Before(deadline) {
			qw.notifier <- NEW_JOB
			break
		}
	}
}

func (q *rocksQueue) clearWaiters() {
	q.waitmu.Lock()
	defer q.waitmu.Unlock()

	for e := q.waiters.Front(); e != nil; e = e.Next() {
		qw := e.Value.(*QueueWaiter)
		qw.notifier <- CLOSE
	}
	q.waiters = list.New()
}

// Waits for a job to come onto the queue.  The given context
// should timeout the blocking.
//
// Waiter calls this method, blocks on a channel it creates.
// Sends that channel to the dispatcher via waiter channel.
// Dispatcher is notified of new job on queue, pulls off a waiter
// and pushes notification to its channel.
func (q *rocksQueue) BPop(ctx context.Context) ([]byte, error) {
	for {
		q.mu.Lock()
		if q.done {
			q.mu.Unlock()
			return nil, nil
		}
		if atomic.LoadUint64(&q.size) > 0 {
			defer q.mu.Unlock()
			return q._pop()
		}
		q.mu.Unlock()

		waiting := make(chan int, 1)
		me := &QueueWaiter{
			ctx:      ctx,
			notifier: waiting,
		}
		q.waitmu.Lock()
		e := q.waiters.PushBack(me)
		q.waitmu.Unlock()

	Loop:
		select {
		case status := <-waiting:
			if status == NEW_JOB {
				continue
			}
			break Loop
		case <-ctx.Done():
			q.waitmu.Lock()
			q.waiters.Remove(e)
			q.waitmu.Unlock()
			return nil, nil
		}
	}
}

func (q *rocksQueue) Delete(keys [][]byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	db := q.store.db
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	defer ro.Destroy()
	defer wo.Destroy()

	var count uint64

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, key := range keys {
		data, err := db.GetCF(ro, q.cf, key)
		if err != nil {
			return err
		}
		if data.Size() > 0 {
			wb.DeleteCF(q.cf, key)
			count++
		}
		data.Free()
	}
	util.Debugf(`Deleting %d elements from queue "%s"`, count, q.name)
	err := db.Write(wo, wb)
	if err == nil {
		// decrement count
		atomic.AddUint64(&q.size, ^uint64(count-1))
	}
	return err
}

//////////////////////////////////////////////////

func (q *rocksQueue) nextkey(priority uint8) []byte {
	p, ok := q.pointers[priority]
	if !ok {
		p = &queuePointer{
			priority: priority,
			high:     0,
			low:      1,
		}
		q.orderedPointers.Insert(p)
		q.pointers[priority] = p
	}

	high := atomic.AddUint64(&p.high, 1)
	return makeKey(q.name, priority, high)
}

func makeKey(name string, priority uint8, seq uint64) []byte {
	bytes := make([]byte, len(name)+1+1+8)
	copy(bytes, name)
	length := len(name) + 1
	bytes[length-1] = 0xFF
	// flip the bits to make sure we can sort with proper priority
	// since we need high priority to have a low numeric value
	// for scanning purposes, this makes higher priority stuff have
	// lower values
	bytes[length+0] = ^byte(priority)
	binary.BigEndian.PutUint64(bytes[length+1:], seq)
	return bytes
}

func decodeKey(name string, key []byte) (string, uint8, uint64) {
	length := len(name) + 1
	// TODO backwards compatibililty, remove after 0.6.0
	if len(key)-len(name) == 9 {
		return name, 5, binary.BigEndian.Uint64(key[length:])
	}
	return name, uint8(^key[length]), binary.BigEndian.Uint64(key[length+1:])
}

func upperBound(name string) []byte {
	bytes := make([]byte, 8+len(name)+1+1+8)
	copy(bytes, name)
	len := len(name) + 1
	bytes[len-1] = 0xFF
	bytes[len+0] = 0xFF
	bytes[len+1] = 0xFF
	bytes[len+2] = 0xFF
	bytes[len+3] = 0xFF
	bytes[len+4] = 0xFF
	bytes[len+5] = 0xFF
	bytes[len+6] = 0xFF
	bytes[len+7] = 0xFF
	bytes[len+8] = 0xFF
	return bytes
}

func queueReadOptions(tailing bool) *gorocksdb.ReadOptions {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetTailing(tailing)
	return ro
}

func queueWriteOptions() *gorocksdb.WriteOptions {
	return gorocksdb.NewDefaultWriteOptions()
}
