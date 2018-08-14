package storage

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
)

const (
	NEW_JOB = iota
	CLOSE
)

type redisQueue struct {
	name    string
	store   *redisStore
	mu      sync.Mutex
	waiters *list.List
	waitmu  sync.RWMutex
	done    bool
}

func (q *redisQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.done = true
	q.clearWaiters()
}

func (q *redisQueue) Name() string {
	return q.name
}

func (q *redisQueue) Page(start int64, count int64, fn func(index int, k, v []byte) error) error {
	index := 0

	slice, err := q.store.client.LRange(q.name, start, start+count).Result()
	for _, job := range slice {
		err = fn(index, nil, []byte(job))
		if err != nil {
			return err
		}
	}
	return err
}

func (q *redisQueue) Each(fn func(index int, k, v []byte) error) error {
	return q.Page(0, -1, fn)
}

func (q *redisQueue) Clear() (uint64, error) {
	count := uint64(0)
	// TODO impl which uses redis range deletes?
	q.mu.Lock()
	defer q.mu.Unlock()

	q.store.client.Del(q.name)

	//util.Warnf("Queue#clear: deleted %d elements from %s, size %d", count, q.name, q.size)
	return count, nil
}

func (q *redisQueue) init() error {
	q.mu = sync.Mutex{}
	q.waiters = list.New()

	util.Debugf("Queue init: %s %d elements", q.name, q.Size())
	return nil
}

func (q *redisQueue) Size() uint64 {
	return uint64(q.store.client.LLen(q.name).Val())
}

func (q *redisQueue) Push(priority uint8, payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.notify()
	return nil
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *redisQueue) Pop() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done {
		return nil, nil
	}

	return q._pop()
}

// caller must hold q.mu or else DOOM!
func (q *redisQueue) _pop() ([]byte, error) {
	return nil, nil
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
func (q *redisQueue) notify() {
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

func (q *redisQueue) clearWaiters() {
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
func (q *redisQueue) BPop(ctx context.Context) ([]byte, error) {
	for {
		q.mu.Lock()
		if q.done {
			q.mu.Unlock()
			return nil, nil
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

func (q *redisQueue) Delete(keys [][]byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	return nil
}
