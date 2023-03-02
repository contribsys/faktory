package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

type redisSorted struct {
	name  string
	store *redisStore
}

func (rs *redisStore) initSorted() {
	rs.scheduled = &redisSorted{name: "scheduled", store: rs}
	rs.retries = &redisSorted{name: "retries", store: rs}
	rs.dead = &redisSorted{name: "dead", store: rs}
	rs.working = &redisSorted{name: "working", store: rs}
}

func (rs *redisSorted) Name() string {
	return rs.name
}

func (rs *redisSorted) Size(ctx context.Context) uint64 {
	return uint64(rs.store.rclient.ZCard(ctx, rs.name).Val())
}

func (rs *redisSorted) Clear(ctx context.Context) error {
	return rs.store.rclient.Unlink(ctx, rs.name).Err()
}

func (rs *redisSorted) Add(ctx context.Context, job *client.Job) error {
	if job.At == "" {
		return errors.New("Job does not have an At timestamp")
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return rs.AddElement(ctx, job.At, job.Jid, data)
}

func (rs *redisSorted) RemoveEntry(ctx context.Context, ent SortedEntry) error {
	return rs.store.rclient.ZRem(ctx, rs.name, ent.Value()).Err()
}

func (rs *redisSorted) AddElement(ctx context.Context, timestamp string, jid string, payload []byte) error {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	_, err = rs.store.rclient.ZAdd(ctx, rs.name, redis.Z{Score: time_f, Member: payload}).Result()
	return err
}

func decompose(key []byte) (float64, string, error) {
	slice := strings.Split(string(key), "|")
	if len(slice) != 2 {
		return 0, "", fmt.Errorf("Invalid key, expected \"timestamp|jid\", not %s", string(key))
	}
	timestamp := slice[0]
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, "", err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	return time_f, slice[1], nil
}

func (rs *redisSorted) getScore(ctx context.Context, score float64) ([]string, error) {
	strf := strconv.FormatFloat(score, 'f', -1, 64)
	elms, err := rs.store.rclient.ZRangeByScore(ctx, rs.name, &redis.ZRangeBy{Min: strf, Max: strf}).Result()
	if err != nil {
		return nil, err
	}
	return elms, nil
}

// key is "timestamp|jid"
func (rs *redisSorted) Get(ctx context.Context, key []byte) (SortedEntry, error) {
	time_f, jid, err := decompose(key)
	if err != nil {
		return nil, err
	}
	elms, err := rs.getScore(ctx, time_f)
	if err != nil {
		return nil, err
	}
	if len(elms) == 0 {
		return nil, nil
	}
	if len(elms) == 1 {
		return NewEntry(time_f, []byte(elms[0])), nil
	}

	for idx := range elms {
		if strings.Index(elms[idx], jid) > 0 {
			return NewEntry(time_f, []byte(elms[idx])), nil
		}
	}
	return nil, nil
}

type setEntry struct {
	value []byte
	score float64
	// these two are lazy-loaded
	job *client.Job
	key []byte
}

func NewEntry(score float64, value []byte) *setEntry {
	return &setEntry{
		value: value,
		score: score,
	}
}

func (e *setEntry) Value() []byte {
	return e.value
}

func (e *setEntry) Key() ([]byte, error) {
	if e.key != nil {
		return e.key, nil
	}
	j, err := e.Job()
	if err != nil {
		return nil, err
	}

	secs := int64(e.score)
	nsecs := int64((e.score - float64(secs)) * 1000000000)
	tim := time.Unix(secs, nsecs)

	e.key = []byte(fmt.Sprintf("%s|%s", util.Thens(tim), j.Jid))
	return e.key, nil
}

func (e *setEntry) Job() (*client.Job, error) {
	if e.job != nil {
		return e.job, nil
	}

	var job client.Job
	err := json.Unmarshal(e.value, &job)
	if err != nil {
		return nil, err
	}

	e.job = &job
	return e.job, nil
}

func (rs *redisSorted) Find(ctx context.Context, match string, fn func(index int, e SortedEntry) error) error {
	it := rs.store.rclient.ZScan(ctx, rs.name, 0, match, 100).Iterator()
	idx := 0
	for it.Next(ctx) {
		job := it.Val()
		if !it.Next(ctx) {
			break
		}
		score := it.Val()
		sf, err := strconv.ParseFloat(score, 64)
		if err != nil {
			return err
		}
		if err := fn(idx, NewEntry(sf, []byte(job))); err != nil {
			return err
		}
		idx += 1
	}
	if err := it.Err(); err != nil {
		return err
	}
	return nil
}

func (rs *redisSorted) Page(ctx context.Context, start int, count int, fn func(index int, e SortedEntry) error) (int, error) {
	zs, err := rs.store.rclient.ZRangeWithScores(ctx, rs.name, int64(start), int64(start+count-1)).Result()
	if err != nil {
		return 0, err
	}

	for idx := range zs {
		err = fn(idx, NewEntry(zs[idx].Score, []byte(zs[idx].Member.(string))))
		if err != nil {
			return idx, err
		}
	}
	return len(zs), nil
}

func (rs *redisSorted) Each(ctx context.Context, fn func(idx int, e SortedEntry) error) error {
	count := 50
	current := 0

	for {
		elms, err := rs.Page(ctx, current, count, fn)
		if err != nil {
			return err
		}

		if elms < count {
			// last page, done iterating
			return nil
		}
		current += count
	}
}

func (rs *redisSorted) rem(ctx context.Context, time_f float64, jid string) (bool, error) {
	elms, err := rs.getScore(ctx, time_f)
	if err != nil {
		return false, err
	}
	if len(elms) == 0 {
		return false, nil
	}
	if len(elms) == 1 {
		count, err := rs.store.rclient.ZRem(ctx, rs.name, elms[0]).Result()
		return count == 1, err
	}

	for idx := range elms {
		if strings.Index(elms[idx], jid) > 0 {
			count, err := rs.store.rclient.ZRem(ctx, rs.name, elms[idx]).Result()
			return count == 1, err
		}
	}
	return false, nil
}

// bool = was it removed?
// err = any error
func (rs *redisSorted) Remove(ctx context.Context, key []byte) (bool, error) {
	time_f, jid, err := decompose(key)
	if err != nil {
		return false, err
	}
	return rs.rem(ctx, time_f, jid)
}

func (rs *redisSorted) RemoveElement(ctx context.Context, timestamp string, jid string) (bool, error) {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return false, err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	return rs.rem(ctx, time_f, jid)
}

func (rs *redisSorted) RemoveBefore(ctx context.Context, timestamp string, maxCount int64, fn func(data []byte) error) (int64, error) {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	strf := strconv.FormatFloat(time_f, 'f', -1, 64)

	vals := rs.store.rclient.ZRangeByScore(ctx, rs.name, &redis.ZRangeBy{Min: "-inf", Max: strf, Count: maxCount})
	jobs, err := vals.Result()
	if err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	count := int64(0)
	for idx := range jobs {
		j := jobs[idx]
		cnt, err := rs.store.rclient.ZRem(ctx, rs.name, j).Result()
		if err != nil {
			return count, err
		}
		if cnt == 1 {
			err = fn([]byte(j))
			if err != nil {
				util.Warnf("Unable to process timed job: %v", err)
				continue
			}
			count++
		}
	}
	return count, nil
}

func (rs *redisSorted) MoveTo(ctx context.Context, sset SortedSet, entry SortedEntry, newtime time.Time) error {
	job, err := entry.Job()
	if err != nil {
		return err
	}

	cnt, err := rs.store.rclient.ZRem(ctx, rs.name, string(entry.Value())).Result()
	if err != nil {
		return err
	}
	if cnt == 0 {
		// race condition, element was removed or moved elsewhere
		return nil
	}

	return sset.AddElement(ctx, util.Thens(newtime), job.Jid, entry.Value())
}
