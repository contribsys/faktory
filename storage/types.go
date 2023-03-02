package storage

import (
	"context"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/redis/go-redis/v9"
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
	ExistingQueue(ctx context.Context, name string) (q Queue, ok bool)
	GetQueue(ctx context.Context, name string) (Queue, error)
	EachQueue(ctx context.Context, eachFn func(Queue))
	Stats(ctx context.Context) map[string]string
	EnqueueAll(ctx context.Context, from SortedSet) error
	EnqueueFrom(ctx context.Context, from SortedSet, data []byte) error
	PausedQueues(ctx context.Context) ([]string, error)

	History(ctx context.Context, days int, fn func(day string, procCnt uint64, failCnt uint64)) error
	Success(ctx context.Context) error
	Failure(ctx context.Context) error
	TotalProcessed(ctx context.Context) uint64
	TotalFailures(ctx context.Context) uint64

	// Clear the database of all job data.
	// Equivalent to Redis's FLUSHDB
	Flush(ctx context.Context) error

	Raw() KV
	Redis
}

type Redis interface {
	Redis() *redis.Client
}

type Queue interface {
	Name() string
	Size(ctx context.Context) uint64

	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	IsPaused(ctx context.Context) bool

	Add(ctx context.Context, job *client.Job) error
	Push(ctx context.Context, data []byte) error

	Pop(ctx context.Context) ([]byte, error)
	BPop(ctx context.Context) ([]byte, error)
	Clear(ctx context.Context) (uint64, error)

	Each(ctx context.Context, fn func(index int, data []byte) error) error
	Page(ctx context.Context, start int64, count int64, fn func(index int, data []byte) error) error

	Delete(ctx context.Context, keys [][]byte) error
}

type SortedEntry interface {
	Value() []byte
	Key() ([]byte, error)
	Job() (*client.Job, error)
}

type SortedSet interface {
	Name() string
	Size(ctx context.Context) uint64
	Clear(ctx context.Context) error

	Add(ctx context.Context, job *client.Job) error
	AddElement(ctx context.Context, timestamp string, jid string, payload []byte) error

	Get(ctx context.Context, key []byte) (SortedEntry, error)
	Page(ctx context.Context, start int, count int, fn func(index int, e SortedEntry) error) (int, error)
	Each(ctx context.Context, fn func(idx int, e SortedEntry) error) error

	Find(ctx context.Context, match string, fn func(idx int, e SortedEntry) error) error

	// bool is whether or not the element was actually removed from the sset.
	// the scheduler and other things can be operating on the sset concurrently
	// so we need to be careful about the data changing under us.
	Remove(ctx context.Context, key []byte) (bool, error)
	RemoveElement(ctx context.Context, timestamp string, jid string) (bool, error)
	RemoveBefore(ctx context.Context, timestamp string, maxCount int64, fn func(data []byte) error) (int64, error)
	RemoveEntry(ctx context.Context, ent SortedEntry) error

	// Move the given key from this SortedSet to the given
	// SortedSet atomically.  The given func may mutate the payload and
	// return a new tstamp.
	MoveTo(ctx context.Context, sset SortedSet, entry SortedEntry, newtime time.Time) error
}
