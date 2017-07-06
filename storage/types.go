package storage

import "fmt"

type Store interface {
	Close() error
	Retries() SortedSet
	Scheduled() SortedSet
	Working() SortedSet
}

var (
	DefaultPath     = "/var/run/worq/"
	ScheduledBucket = "scheduled"
	RetriesBucket   = "retries"
	WorkingBucket   = "working"
)

type SortedSet interface {
	AddElement(timestamp string, jid string, payload []byte) error
	RemoveElement(timestamp string, jid string) error
	RemoveBefore(timestamp string) ([][]byte, error)
	Size() int
	EachElement(func(string, string, []byte) error) error

	/*
		Move the given tstamp/jid pair from this SortedSet to the given
		SortedSet atomically.  The given func may mutate the payload and
		return a new tstamp.
	*/
	MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error
}

func Open(dbtype string, path string) (Store, error) {
	if dbtype == "boltdb" {
		return OpenBolt(path)
	} else if dbtype == "rocksdb" {
		return OpenRocks(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
