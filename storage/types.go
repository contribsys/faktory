package storage

import "fmt"

type Store interface {
	Close() error
	Retries() TimedSet
	Scheduled() TimedSet
	Working() TimedSet
}

var (
	DefaultPath     = "/var/run/worq/"
	ScheduledBucket = "scheduled"
	RetriesBucket   = "retries"
	WorkingBucket   = "working"
)

type TimedSet interface {
	AddElement(timestamp string, jid string, payload []byte) error
	RemoveElement(timestamp string, jid string) error
	RemoveBefore(timestamp string) ([][]byte, error)
	Size() int
}

func Open(dbtype string, path string) (Store, error) {
	if dbtype == "rocksdb" {
		return OpenRocks(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
