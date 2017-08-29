package storage

import "fmt"

type Store interface {
	Close() error
	Retries() SortedSet
	Scheduled() SortedSet
	Working() SortedSet
	Dead() SortedSet
	Clients() SortedSet
	GetQueue(string) (Queue, error)
	EachQueue(func(Queue))
	Stats() map[string]string
}

type Queue interface {
	Name() string
	Size() int64
	Push([]byte) error
	Pop() ([]byte, error)
	Each(func(index int, k, v []byte) error) error
	Page(int64, int64, func(index int, k, v []byte) error) error
	Clear() (int, error)
}

var (
	DefaultPath     = "/var/run/faktory/"
	ScheduledBucket = "scheduled"
	RetriesBucket   = "retries"
	WorkingBucket   = "working"
	DeadBucket      = "dead"
)

type SortedSet interface {
	AddElement(timestamp string, jid string, payload []byte) error
	RemoveElement(timestamp string, jid string) error
	RemoveBefore(timestamp string) ([][]byte, error)
	Size() int64
	Page(int64, int64, func(index int, key string, data []byte) error) error
	Clear() (int64, error)

	Remove(key string) error
	Get(key string) ([]byte, error)
	Each(func(idx int, key string, data []byte) error) error

	/*
		Move the given tstamp/jid pair from this SortedSet to the given
		SortedSet atomically.  The given func may mutate the payload and
		return a new tstamp.
	*/
	MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error
}

func Open(dbtype string, path string) (Store, error) {
	if dbtype == "rocksdb" {
		return OpenRocks(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
