package storage

import (
	"context"
	"fmt"

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
}

type Redis interface {
	Redis() *redis.Client
}

type Queue interface {
	Name() string
	Size() uint64

	Add(job *client.Job) error
	Push(priority uint8, data []byte) error

	Pop() ([]byte, error)
	BPop(context.Context) ([]byte, error)
	Clear() (uint64, error)

	// Please note that k/vs are NOT safe to use outside of the func.
	// You must copy the values if you want to stash them for later use.
	//
	//	  cpy = make([]byte, len(k))
	//	  copy(cpy, k)
	Each(func(index int, k, v []byte) error) error
	Page(int64, int64, func(index int, k, v []byte) error) error

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

	Get(key []byte) ([]byte, error)
	Page(start int, count int, fn func(index int, e SortedEntry) error) (int, error)
	Each(fn func(idx int, e SortedEntry) error) error

	Remove(key []byte) error
	RemoveElement(timestamp string, jid string) error
	RemoveBefore(timestamp string) ([][]byte, error)

	// Move the given key from this SortedSet to the given
	// SortedSet atomically.  The given func may mutate the payload and
	// return a new tstamp.
	MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error
}

func Open(dbtype string, path string) (Store, error) {
	if dbtype == "redis" {
		return OpenRedis(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
