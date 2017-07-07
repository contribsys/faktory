package storage

import (
	"fmt"

	"github.com/mperham/gorocksdb"
)

type RocksStore struct {
	Name      string
	db        *gorocksdb.DB
	retries   *RocksSortedSet
	scheduled *RocksSortedSet
	working   *RocksSortedSet
	defalt    *gorocksdb.ColumnFamilyHandle
	queues    *gorocksdb.ColumnFamilyHandle
	queueSet  map[string]*RocksQueue
}

func OpenRocks(path string) (Store, error) {
	if path == "" {
		path = fmt.Sprintf("%s/%s", DefaultPath, "default")
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	db, handles, err := gorocksdb.OpenDbColumnFamilies(opts, path,
		[]string{ScheduledBucket, RetriesBucket, WorkingBucket, "default", "queues"},
		[]*gorocksdb.Options{opts, opts, opts, opts, opts})
	if err != nil {
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	return &RocksStore{
		Name:      path,
		db:        db,
		scheduled: &RocksSortedSet{Name: ScheduledBucket, db: db, cf: handles[0], ro: ro, wo: wo},
		retries:   &RocksSortedSet{Name: RetriesBucket, db: db, cf: handles[1], ro: ro, wo: wo},
		working:   &RocksSortedSet{Name: WorkingBucket, db: db, cf: handles[2], ro: ro, wo: wo},
		defalt:    handles[3],
		queues:    handles[4],
		queueSet:  make(map[string]*RocksQueue),
	}, nil
}

func (store *RocksStore) GetQueue(name string) Queue {
	q, ok := store.queueSet[name]
	if ok {
		return q
	}
	q = &RocksQueue{
		Name:  name,
		size:  -1,
		store: store,
		cf:    store.queues,
	}
	store.queueSet[name] = q
	return q
}

func (store *RocksStore) Close() error {
	store.retries.Close()
	store.working.Close()
	store.scheduled.Close()
	store.defalt.Destroy()
	store.queues.Destroy()
	store.db.Close()
	return nil
}

func (store *RocksStore) Retries() SortedSet {
	return store.retries
}

func (store *RocksStore) Scheduled() SortedSet {
	return store.scheduled
}

func (store *RocksStore) Working() SortedSet {
	return store.working
}
