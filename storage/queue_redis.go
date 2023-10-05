package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

type redisQueue struct {
	name  string
	store *redisStore
	done  bool
}

func (store *redisStore) NewQueue(name string) *redisQueue {
	return &redisQueue{
		name:  name,
		store: store,
		done:  false,
	}
}

func (q *redisQueue) Pause(ctx context.Context) error {
	return q.store.rclient.SAdd(ctx, "paused", q.name).Err()
}

func (q *redisQueue) Resume(ctx context.Context) error {
	return q.store.rclient.SRem(ctx, "paused", q.name).Err()
}

func (q *redisQueue) IsPaused(ctx context.Context) bool {
	b, _ := q.store.rclient.SIsMember(ctx, "paused", q.name).Result()
	return b
}

func (q *redisQueue) Close() {
	q.done = true
}

func (q *redisQueue) Name() string {
	return q.name
}

func (q *redisQueue) Page(ctx context.Context, start int64, count int64, fn func(index int, data []byte) error) error {
	index := 0

	slice, err := q.store.rclient.LRange(ctx, q.name, start, start+count).Result()
	for idx := range slice {
		err = fn(index, []byte(slice[idx]))
		if err != nil {
			return err
		}
		index += 1
	}
	return err
}

func (q *redisQueue) Each(ctx context.Context, fn func(index int, data []byte) error) error {
	return q.Page(ctx, 0, -1, fn)
}

func (q *redisQueue) Clear(ctx context.Context) (uint64, error) {
	q.store.mu.Lock()
	defer q.store.mu.Unlock()

	_, err := q.store.rclient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Unlink(ctx, q.name)
		pipe.SRem(ctx, "queues", q.name)
		pipe.SRem(ctx, "paused", q.name)
		return nil
	})
	if err != nil {
		return 0, err
	}

	delete(q.store.queueSet, q.name)
	return 0, nil
}

func (q *redisQueue) init(ctx context.Context) error {
	util.Debugf("Queue init: %s %d elements", q.name, q.Size(ctx))
	return nil
}

func (q *redisQueue) Size(ctx context.Context) uint64 {
	return uint64(q.store.rclient.LLen(ctx, q.name).Val())
}

func (q *redisQueue) Add(ctx context.Context, job *client.Job) error {
	job.EnqueuedAt = util.Nows()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return q.Push(ctx, data)
}

func (q *redisQueue) Push(ctx context.Context, payload []byte) error {
	return q.store.rclient.LPush(ctx, q.name, payload).Err()
}

// non-blocking, returns immediately if there's nothing enqueued
func (q *redisQueue) Pop(ctx context.Context) ([]byte, error) {
	if q.done {
		return nil, nil
	}

	return q._pop(ctx)
}

func (q *redisQueue) _pop(ctx context.Context) ([]byte, error) {
	val, err := q.store.rclient.RPop(ctx, q.name).Result()
	if val == "" {
		return nil, nil
	}
	return []byte(val), err
}

func (q *redisQueue) BPop(ctx context.Context) ([]byte, error) {
	val, err := q.store.rclient.BRPop(ctx, 2*time.Second, q.name).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	return []byte(val[1]), nil
}

func (q *redisQueue) Delete(ctx context.Context, vals [][]byte) error {
	for idx := range vals {
		err := q.store.rclient.LRem(ctx, q.name, 1, vals[idx]).Err()
		if err != nil {
			return err
		}
	}

	return nil
}
