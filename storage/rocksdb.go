package storage

import (
	"fmt"

	"github.com/mperham/gorocksdb"
)

type RocksStore struct {
	Name      string
	db        *gorocksdb.DB
	retries   *RocksTimedSet
	scheduled *RocksTimedSet
	working   *RocksTimedSet
}

func OpenRocks(path string) (Store, error) {
	if path == "" {
		path = fmt.Sprintf("%s/%s", DefaultPath, "default")
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	db, handles, err := gorocksdb.OpenDbColumnFamilies(opts, path,
		[]string{ScheduledBucket, RetriesBucket, WorkingBucket, "default"},
		[]*gorocksdb.Options{opts, opts, opts, opts})
	if err != nil {
		return nil, err
	}
	handles[3].Destroy()

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	return &RocksStore{
		Name:      path,
		db:        db,
		scheduled: &RocksTimedSet{Name: ScheduledBucket, db: db, cf: handles[0], ro: ro, wo: wo},
		retries:   &RocksTimedSet{Name: RetriesBucket, db: db, cf: handles[1], ro: ro, wo: wo},
		working:   &RocksTimedSet{Name: WorkingBucket, db: db, cf: handles[2], ro: ro, wo: wo},
	}, nil
}

func (store *RocksStore) Close() error {
	store.retries.Close()
	store.working.Close()
	store.scheduled.Close()
	store.db.Close()
	return nil
}

func (store *RocksStore) Retries() TimedSet {
	return store.retries
}

func (store *RocksStore) Scheduled() TimedSet {
	return store.scheduled
}

func (store *RocksStore) Working() TimedSet {
	return store.working
}
