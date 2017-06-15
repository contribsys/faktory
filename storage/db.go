package storage

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type Store struct {
	Name      string
	keyspace  *bolt.DB
	scheduled *bolt.Bucket
	retries   *bolt.Bucket
}

var (
	DefaultPath = "/var/run/worq/default.db"
)

func OpenStore(path string) (*Store, error) {
	if path == "" {
		path = DefaultPath
	}
	db, err := bolt.Open(path, 0600,
		&bolt.Options{Timeout: 1 * time.Millisecond})
	if err != nil {
		return nil, err
	}

	var sched *bolt.Bucket
	var retry *bolt.Bucket

	err = db.Update(func(tx *bolt.Tx) error {
		retry, err = tx.CreateBucketIfNotExists([]byte("Retries"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		sched, err = tx.CreateBucketIfNotExists([]byte("Scheduled"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		Name:      "default",
		keyspace:  db,
		scheduled: sched,
		retries:   retry,
	}, nil
}
