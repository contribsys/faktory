package storage

import (
	"context"
	"time"

	"github.com/contribsys/faktory/util"
)

type redisQueue struct {
	name  string
	store *redisStore
	done  bool
}

func (q *redisQueue) Close() {
	q.done = true
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
	q.store.client.Del(q.name)
	return 0, nil
}

func (q *redisQueue) init() error {
	util.Debugf("Queue init: %s %d elements", q.name, q.Size())
	return nil
}

func (q *redisQueue) Size() uint64 {
	return uint64(q.store.client.LLen(q.name).Val())
}

func (q *redisQueue) Push(priority uint8, payload []byte) error {
	q.store.client.LPush(q.name, payload)
	return nil
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *redisQueue) Pop() ([]byte, error) {
	if q.done {
		return nil, nil
	}

	return q._pop()
}

func (q *redisQueue) _pop() ([]byte, error) {
	val, err := q.store.client.RPop(q.name).Result()
	return []byte(val), err
}

func (q *redisQueue) BPop(ctx context.Context) ([]byte, error) {
	val, err := q.store.client.BRPop(2*time.Second, q.name).Result()
	if err != nil {
		return nil, err
	}

	return []byte(val[1]), nil
}

func (q *redisQueue) Delete(vals [][]byte) error {
	for _, val := range vals {
		err := q.store.client.LRem(q.name, 1, val).Err()
		if err != nil {
			return err
		}
	}

	return nil
}
