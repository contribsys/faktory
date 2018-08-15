package storage

import (
	"fmt"
	"time"
)

func (store *redisStore) Success() error {
	daystr := time.Now().Format("2006-01-02")
	store.client.Incr(fmt.Sprintf("Processed:%s", daystr))
	store.client.Incr("Processed")
	return nil
}

func (store *redisStore) TotalProcessed() uint64 {
	return uint64(store.client.IncrBy("Processed", 0).Val())
}
func (store *redisStore) TotalFailures() uint64 {
	return uint64(store.client.IncrBy("Failures", 0).Val())
}

func (store *redisStore) Failure() error {
	store.client.Incr("Processed")
	store.client.Incr("Failures")

	daystr := time.Now().Format("2006-01-02")
	store.client.Incr(fmt.Sprintf("Processed:%s", daystr))
	store.client.Incr(fmt.Sprintf("Failures:%s", daystr))
	return nil
}

func (store *redisStore) History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error {
	ts := time.Now()
	daystr := ts.Format("2006-01-02")

	var proc uint64
	var failed uint64
	value, err := store.client.IncrBy(fmt.Sprintf("Processed:%s", daystr), 0).Result()
	if err != nil {
		return err
	}
	proc = uint64(value)
	value, err = store.client.IncrBy(fmt.Sprintf("Failures:%s", daystr), 0).Result()
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
		proc, err := store.client.IncrBy(fmt.Sprintf("Processed:%s", daystr), 0).Result()
		if err != nil {
			return err
		}
		failed, err := store.client.IncrBy(fmt.Sprintf("Failures:%s", daystr), 0).Result()
		if err != nil {
			return err
		}
		fn(daystr, uint64(proc), uint64(failed))
		proc = 0
		failed = 0
	}
	return nil
}
