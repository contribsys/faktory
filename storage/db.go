package storage

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type Store struct {
	Name string
	db   *bolt.DB
}

var (
	DefaultPath     = "/var/run/worq/"
	ScheduledBucket = "scheduled"
	RetriesBucket   = "retries"
	WorkingBucket   = "working"
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

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(RetriesBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(ScheduledBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(WorkingBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		Name: "default",
		db:   db,
	}, nil
}

func (store *Store) Retries() *TimedSet {
	return &TimedSet{RetriesBucket, store.db}
}

func (store *Store) Scheduled() *TimedSet {
	return &TimedSet{ScheduledBucket, store.db}
}

func (store *Store) Working() *TimedSet {
	return &TimedSet{WorkingBucket, store.db}
}
