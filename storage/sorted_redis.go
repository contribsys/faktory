package storage

import (
	"errors"
	"strconv"
	"strings"

	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
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

func (rs *redisSorted) Size() uint64 {
	return uint64(rs.store.client.ZCard(rs.name).Val())
}

func (rs *redisSorted) Clear() error {
	return rs.store.client.Del(rs.name).Err()
}

func (rs *redisSorted) AddElement(timestamp string, jid string, payload []byte) error {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	_, err = rs.store.client.ZAdd(rs.name, redis.Z{Score: time_f, Member: payload}).Result()
	return err
}

func decompose(key []byte) (float64, string, error) {
	slice := strings.Split(string(key), "|")
	if len(slice) != 2 {
		return 0, "", errors.New("Invalid key, expected \"timestamp|jid\"")
	}
	timestamp := slice[0]
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return 0, "", err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	return time_f, slice[1], nil
}

func (rs *redisSorted) getScore(score float64) ([]string, error) {
	strf := strconv.FormatFloat(score, 'f', -1, 64)
	elms, err := rs.store.client.ZRangeByScore(rs.name, redis.ZRangeBy{Min: strf, Max: strf}).Result()
	if err != nil {
		return nil, err
	}
	return elms, nil
}

// key is "timestamp|jid"
func (rs *redisSorted) Get(key []byte) ([]byte, error) {
	time_f, jid, err := decompose(key)
	if err != nil {
		return nil, err
	}
	elms, err := rs.getScore(time_f)
	if err != nil {
		return nil, err
	}
	if len(elms) == 0 {
		return nil, nil
	}
	if len(elms) == 1 {
		return []byte(elms[0]), nil
	}

	for _, elm := range elms {
		if strings.Index(elm, jid) > 0 {
			return []byte(elm), nil
		}
	}
	return nil, nil
}

func (rs *redisSorted) Page(int64, int64, func(index int, key []byte, data []byte) error) error {
	return errors.New("ZBoom")
}

func (rs *redisSorted) Each(fn func(idx int, key []byte, data []byte) error) error {
	idx := 0
	zcursor := uint64(0)

	for {
		jobs, cursor, err := rs.store.client.ZScan(rs.name, zcursor, "*", 50).Result()
		if err != nil {
			return err
		}
		for _, job := range jobs {
			err = fn(idx, nil, []byte(job))
			if err != nil {
				return err
			}
			idx += 1
		}
		if cursor == 0 {
			break
		}
		zcursor = cursor
	}
	return nil
}

func (rs *redisSorted) rem(time_f float64, jid string) error {
	elms, err := rs.getScore(time_f)
	if err != nil {
		return err
	}
	if len(elms) == 0 {
		return nil
	}
	if len(elms) == 1 {
		return rs.store.client.ZRem(rs.name, elms[0]).Err()
	}

	for _, elm := range elms {
		if strings.Index(elm, jid) > 0 {
			return rs.store.client.ZRem(rs.name, elm).Err()
		}
	}
	return nil
}

func (rs *redisSorted) Remove(key []byte) error {
	time_f, jid, err := decompose(key)
	if err != nil {
		return err
	}
	return rs.rem(time_f, jid)
}

func (rs *redisSorted) RemoveElement(timestamp string, jid string) error {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	return rs.rem(time_f, jid)
}

func (rs *redisSorted) RemoveBefore(timestamp string) ([][]byte, error) {
	tim, err := util.ParseTime(timestamp)
	if err != nil {
		return nil, err
	}
	time_f := float64(tim.Unix()) + (float64(tim.Nanosecond()) / 1000000000)
	strf := strconv.FormatFloat(time_f, 'f', -1, 64)

	var vals *redis.StringSliceCmd
	_, err = rs.store.client.TxPipelined(func(pipe redis.Pipeliner) error {
		vals = pipe.ZRangeByScore(rs.name, redis.ZRangeBy{Min: "-inf", Max: strf})
		pipe.ZRemRangeByScore(rs.name, "-inf", strf)
		return nil
	})
	if err != nil {
		return nil, err
	}
	jobs, err := vals.Result()
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return [][]byte{}, nil
	}

	results := make([][]byte, len(jobs))
	for idx, j := range jobs {
		results[idx] = []byte(j)
	}
	return results, nil
}

// Move the given key from this SortedSet to the given
// SortedSet atomically.  The given func may mutate the payload and
// return a new tstamp.
func (rs *redisSorted) MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error {
	return nil
}
