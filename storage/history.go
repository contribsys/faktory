package storage

import (
	"fmt"
	"time"
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
	daystr := ts.Format("2006-01-02")

	var proc uint64
	var failed uint64
	value, err := store.rclient.IncrBy(fmt.Sprintf("processed:%s", daystr), 0).Result()
	if err != nil {
		return err
	}
	proc = uint64(value)
	value, err = store.rclient.IncrBy(fmt.Sprintf("failures:%s", daystr), 0).Result()
	if err != nil {
		return err
	}
	failed = uint64(value)
	fn(daystr, proc, failed)
	proc = 0
	failed = 0

	for i := 1; i < days; i++ {
		ts = ts.Add(-24 * time.Hour)
		daystr = ts.Format("2006-01-02")
		proc, err := store.rclient.IncrBy(fmt.Sprintf("processed:%s", daystr), 0).Result()
		if err != nil {
			return err
		}
		failed, err := store.rclient.IncrBy(fmt.Sprintf("failures:%s", daystr), 0).Result()
		if err != nil {
			return err
		}
		fn(daystr, uint64(proc), uint64(failed))
		proc = 0
		failed = 0
	}
	return nil
}
