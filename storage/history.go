package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (store *redisStore) Success(ctx context.Context) error {
	daystr := time.Now().Format("2006-01-02")
	store.rclient.Incr(ctx, fmt.Sprintf("processed:%s", daystr))
	store.rclient.Incr(ctx, "processed")
	return nil
}

func (store *redisStore) TotalProcessed(ctx context.Context) uint64 {
	return uint64(store.rclient.IncrBy(ctx, "processed", 0).Val())
}
func (store *redisStore) TotalFailures(ctx context.Context) uint64 {
	return uint64(store.rclient.IncrBy(ctx, "failures", 0).Val())
}

func (store *redisStore) Failure(ctx context.Context) error {
	store.rclient.Incr(ctx, "processed")
	store.rclient.Incr(ctx, "failures")

	daystr := time.Now().Format("2006-01-02")
	store.rclient.Incr(ctx, fmt.Sprintf("processed:%s", daystr))
	store.rclient.Incr(ctx, fmt.Sprintf("failures:%s", daystr))
	return nil
}

func (store *redisStore) History(ctx context.Context, days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	if days > 180 {
		return errors.New("days value can't be greater than 180")
	}
	ts := time.Now()
	daystrs := make([]string, days)
	fails := make([]*redis.IntCmd, days)
	procds := make([]*redis.IntCmd, days)

	_, err := store.rclient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for idx := 0; idx < days; idx++ {
			daystr := ts.Format("2006-01-02")
			daystrs[idx] = daystr
			procds[idx] = pipe.IncrBy(ctx, fmt.Sprintf("processed:%s", daystr), 0)
			fails[idx] = pipe.IncrBy(ctx, fmt.Sprintf("failures:%s", daystr), 0)
			ts = ts.Add(-24 * time.Hour)
		}
		return nil
	})
	if err != nil {
		return err
	}

	for idx := 0; idx < days; idx++ {
		fn(daystrs[idx], uint64(procds[idx].Val()), uint64(fails[idx].Val()))
	}
	return nil
}
