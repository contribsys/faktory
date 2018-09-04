package storage

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

func (store *redisStore) Success() error {
	daystr := time.Now().Format("2006-01-02")
	store.rclient.Incr(fmt.Sprintf("processed:%s", daystr))
	store.rclient.Incr("processed")
	return nil
}

func (store *redisStore) TotalProcessed() uint64 {
	return uint64(store.rclient.IncrBy("processed", 0).Val())
}
func (store *redisStore) TotalFailures() uint64 {
	return uint64(store.rclient.IncrBy("failures", 0).Val())
}

func (store *redisStore) Failure() error {
	store.rclient.Incr("processed")
	store.rclient.Incr("failures")

	daystr := time.Now().Format("2006-01-02")
	store.rclient.Incr(fmt.Sprintf("processed:%s", daystr))
	store.rclient.Incr(fmt.Sprintf("failures:%s", daystr))
	return nil
}

func (store *redisStore) History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	ts := time.Now()
	daystrs := make([]string, days)
	fails := make([]*redis.IntCmd, days)
	procds := make([]*redis.IntCmd, days)

	_, err := store.rclient.Pipelined(func(pipe redis.Pipeliner) error {
		for idx := 0; idx < days; idx++ {
			daystr := ts.Format("2006-01-02")
			daystrs[idx] = daystr
			procds[idx] = pipe.IncrBy(fmt.Sprintf("processed:%s", daystr), 0)
			fails[idx] = pipe.IncrBy(fmt.Sprintf("failures:%s", daystr), 0)
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
