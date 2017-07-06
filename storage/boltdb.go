package storage

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type BoltStore struct {
	Name      string
	db        *bolt.DB
	retries   *BoltSortedSet
	scheduled *BoltSortedSet
	working   *BoltSortedSet
}

func OpenBolt(path string) (Store, error) {
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

	return &BoltStore{
		Name:      path,
		db:        db,
		retries:   &BoltSortedSet{Name: RetriesBucket, db: db},
		scheduled: &BoltSortedSet{Name: ScheduledBucket, db: db},
		working:   &BoltSortedSet{Name: WorkingBucket, db: db},
	}, nil
}

func (store *BoltStore) Close() error {
	return store.db.Close()
}

func (store *BoltStore) Retries() SortedSet {
	return store.retries
}

func (store *BoltStore) Scheduled() SortedSet {
	return store.scheduled
}

func (store *BoltStore) Working() SortedSet {
	return store.working
}
