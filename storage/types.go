package storage

import (
	"context"
	"fmt"
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

	History(days int, fn func(day string, procCnt int64, failCnt int64)) error
	Success() error
	Processed() int64
	Failure() error
	Failures() int64

	// creates a backup of the current database
	Backup() error
	EachBackup(func(bi BackupInfo)) error
	RestoreFromLatest() error
	PurgeOldBackups(int) error

	// Clear the database of all job data.
	// Equivalent to Redis's FLUSHDB
	Flush() error
}

type Queue interface {
	Name() string
	Size() uint64
	Push(uint8, []byte) error
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

type SortedSet interface {
	Name() string
	Size() int64
	Clear() (int64, error)

	AddElement(timestamp string, jid string, payload []byte) error

	Get(key []byte) ([]byte, error)
	Page(int64, int64, func(index int, key []byte, data []byte) error) error
	Each(func(idx int, key []byte, data []byte) error) error

	Remove(key []byte) error
	RemoveElement(timestamp string, jid string) error
	RemoveBefore(timestamp string) ([][]byte, error)

	// Move the given key from this SortedSet to the given
	// SortedSet atomically.  The given func may mutate the payload and
	// return a new tstamp.
	MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error
}

func Open(dbtype string, path string) (Store, error) {
	if dbtype == "rocksdb" {
		return OpenRocks(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
