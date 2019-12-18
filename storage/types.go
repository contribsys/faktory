package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/go-redis/redis"
)

type BackupInfo struct {
	Id        int64
	FileCount int32
	Size      int64
	Timestamp int64
}

type Store interface {
	Close() error
	Retries() SortedSet
	Scheduled() SortedSet
	Working() SortedSet
	Dead() SortedSet
	GetQueue(string) (Queue, error)
	EachQueue(func(Queue))
	Stats() map[string]string
	EnqueueAll(SortedSet) error
	EnqueueFrom(SortedSet, []byte) error

	History(days int, fn func(day string, procCnt uint64, failCnt uint64)) error
	Success() error
	Failure() error
	TotalProcessed() uint64
	TotalFailures() uint64

	// Clear the database of all job data.
	// Equivalent to Redis's FLUSHDB
	Flush() error

	Raw() KV
	Redis
}

type Redis interface {
	Redis() *redis.Client
}

type Queue interface {
	Name() string
	Size() uint64

	Add(job *client.Job) error
	Push(data []byte) error

	Pop() ([]byte, error)
	BPop(context.Context) ([]byte, error)
	Clear() (uint64, error)

	Each(func(index int, data []byte) error) error
	Page(start int64, count int64, fn func(index int, data []byte) error) error

	Delete(keys [][]byte) error
}

type SortedEntry interface {
	Value() []byte
	Key() ([]byte, error)
	Job() (*client.Job, error)
}

type SortedSet interface {
	Name() string
	Size() uint64
	Clear() error

	Add(job *client.Job) error
	AddElement(timestamp string, jid string, payload []byte) error

	Get(key []byte) (SortedEntry, error)
	Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error)
	Each(fn func(idx int, e SortedEntry) error) error

	Find(match string, fn func(idx int, e SortedEntry) error) error

	// bool is whether or not the element was actually removed from the sset.
	// the scheduler and other things can be operating on the sset concurrently
	// so we need to be careful about the data changing under us.
	Remove(key []byte) (bool, error)
	RemoveElement(timestamp string, jid string) (bool, error)
	RemoveBefore(timestamp string) ([][]byte, error)
	RemoveEntry(ent SortedEntry) error

	// Move the given key from this SortedSet to the given
	// SortedSet atomically.  The given func may mutate the payload and
	// return a new tstamp.
	MoveTo(sset SortedSet, entry SortedEntry, newtime time.Time) error
}

func Open(dbtype string, path string, size int) (Store, error) {
	if dbtype == "redis" {
		return OpenRedis(path, size)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
