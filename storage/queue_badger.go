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
	"github.com/dgraph-io/badger"
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

type bQueue struct {
	// includes the q~ namespace prefix
	name  string
	store *bStore

	size    uint64
	mu      sync.Mutex
	maxsz   uint64
	waiters *list.List
	waitmu  sync.RWMutex
	done    bool

	orderedPointers *brodal.Heap
	pointers        map[uint8]*queuePointer
}

func newQueue(name string, store *bStore) (*bQueue, error) {
	q := &bQueue{
		name:            fmt.Sprintf("%s%s", "q~", name),
		store:           store,
		mu:              sync.Mutex{},
		waiters:         list.New(),
		pointers:        make(map[uint8]*queuePointer),
		orderedPointers: brodal.NewHeap(),
		maxsz:           DefaultMaxSize,
	}
	err := q.init()
	if err != nil {
		return nil, err
	}
	return q, nil
}

func (q *bQueue) init() error {
	var count uint64
	err := q.store.bdb.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := append([]byte(q.name), 0xFF)
		it.Seek(prefix)

		for ; it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()

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

			count++
		}
		return nil
	})
	if err != nil {
		return err
	}

	atomic.StoreUint64(&q.size, count)

	util.Debugf("Queue init: %s %d elements", q.name, atomic.LoadUint64(&q.size))
	return nil
}

func (q *bQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.clearWaiters()
}

func (q *bQueue) Name() string {
	// don't include the q~ namespace prefix
	return q.name[2:]
}

func (q *bQueue) Page(start int64, count int64, fn func(index int, k, v []byte) error) error {
	index := 0
	//upper := upperBound(q.name)

	return q.store.bdb.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := append([]byte(q.name), 0xFF)
		it.Seek(prefix)

		// skip any before start point
		for i := start; i > 0; i-- {
			if !it.ValidForPrefix(prefix) {
				return nil
			}
			it.Next()
		}

		for ; it.ValidForPrefix(prefix); it.Next() {
			if count == 0 {
				break
			}

			k := it.Item().Key()
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			err = fn(index, k[2:], v)
			index++
			if err != nil {
				return err
			}
			count -= 1
		}
		return nil
	})
}

func (q *bQueue) Each(fn func(index int, k, v []byte) error) error {
	return q.Page(0, -1, fn)
}

func (q *bQueue) Clear() (uint64, error) {
	count := uint64(0)
	q.mu.Lock()
	defer q.mu.Unlock()

	//upper := upperBound(q.name)

	err := q.store.bdb.Update(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()

		prefix := append([]byte(q.name), 0xFF)
		it.Seek(prefix)

		for ; it.Valid(); it.Next() {
			err := tx.Delete(it.Item().Key())
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// reset the pointer references since we iterated over all keys
	q.orderedPointers = brodal.NewHeap()
	q.pointers = make(map[uint8]*queuePointer)
	atomic.StoreUint64(&q.size, 0)

	//util.Warnf("Queue#clear: deleted %d elements from %s, size %d", count, q.name, q.size)
	return count, nil
}

func (q *bQueue) Size() uint64 {
	return atomic.LoadUint64(&q.size)
}

func (q *bQueue) Push(priority uint8, payload []byte) error {
	size := atomic.LoadUint64(&q.size)
	if size > q.maxsz {
		return Backpressure{q.name, size, q.maxsz}
	}

	q.mu.Lock()
	k := q.nextkey(priority)
	q.mu.Unlock()

	v := payload
	err := q.store.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(k, v)
	})
	if err != nil {
		return err
	}

	//util.Warnf("Adding element %x to %s", k, q.name)
	atomic.AddUint64(&q.size, 1)

	q.notify()
	return nil
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *bQueue) Pop() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if atomic.LoadUint64(&q.size) == 0 || q.done {
		return nil, nil
	}

	return q._pop()
}

// caller must hold q.mu or else DOOM!
func (q *bQueue) _pop() ([]byte, error) {
	var value []byte
	var err error

	// got nothing
	if len(q.pointers) == 0 {
		return nil, nil
	}

	p := q.orderedPointers.Peek().(*queuePointer)

	var key []byte
	for {
		key = makeKey(q.name, p.priority, p.low)
		err := q.store.bdb.View(func(tx *badger.Txn) error {
			item, err := tx.Get(key)
			if err != nil {
				return err
			}
			value, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
			return nil
		})
		if err == badger.ErrKeyNotFound {
			err = nil
			value = nil
		}
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

	err = q.store.bdb.Update(func(tx *badger.Txn) error {
		return tx.Delete(key)
	})
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
func (q *bQueue) notify() {
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

func (q *bQueue) clearWaiters() {
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
func (q *bQueue) BPop(ctx context.Context) ([]byte, error) {
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

func (q *bQueue) Delete(keys [][]byte) error {
	var count uint64

	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.store.bdb.Update(func(tx *badger.Txn) error {
		for _, key := range keys {
			qkey := append([]byte("q~"), key...)
			_, err := tx.Get(qkey)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}

			err = tx.Delete(qkey)
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	util.Infof(`Deleting %d elements from queue "%s"`, count, q.name)
	if err == nil {
		// decrement count
		atomic.AddUint64(&q.size, ^uint64(count-1))
	}
	return err
}

//////////////////////////////////////////////////

func (q *bQueue) nextkey(priority uint8) []byte {
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
