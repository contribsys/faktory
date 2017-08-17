package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/mperham/faktory/util"
	"github.com/mperham/gorocksdb"
)

type rocksStore struct {
	Name      string
	db        *gorocksdb.DB
	retries   *rocksSortedSet
	scheduled *rocksSortedSet
	working   *rocksSortedSet
	dead      *rocksSortedSet
	defalt    *gorocksdb.ColumnFamilyHandle
	queues    *gorocksdb.ColumnFamilyHandle
	queueSet  map[string]*rocksQueue
	mu        sync.Mutex
}

func OpenRocks(path string) (Store, error) {
	if path == "" {
		path = "default"
	}
	fullpath := fmt.Sprintf("%s/%s", DefaultPath, path)
	util.Infof("Initializing storage at %s", fullpath)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	db, handles, err := gorocksdb.OpenDbColumnFamilies(opts, fullpath,
		[]string{ScheduledBucket, RetriesBucket, WorkingBucket, DeadBucket, "default", "queues"},
		[]*gorocksdb.Options{opts, opts, opts, opts, opts, opts})
	if err != nil {
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	rs := &rocksStore{
		Name:      path,
		db:        db,
		scheduled: (&rocksSortedSet{Name: ScheduledBucket, db: db, cf: handles[0], ro: ro, wo: wo}).init(),
		retries:   (&rocksSortedSet{Name: RetriesBucket, db: db, cf: handles[1], ro: ro, wo: wo}).init(),
		working:   (&rocksSortedSet{Name: WorkingBucket, db: db, cf: handles[2], ro: ro, wo: wo}).init(),
		dead:      (&rocksSortedSet{Name: DeadBucket, db: db, cf: handles[3], ro: ro, wo: wo}).init(),
		defalt:    handles[4],
		queues:    handles[5],
		queueSet:  make(map[string]*rocksQueue),
		mu:        sync.Mutex{},
	}
	err = rs.init()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (store *rocksStore) Stats() map[string]string {
	return map[string]string{
		"stats": store.db.GetProperty("rocksdb.stats"),
	}
}

// queues are iterated in sorted, lexigraphical order
func (store *rocksStore) EachQueue(x func(Queue)) {
	store.mu.Lock()
	keys := make([]string, 0, len(store.queueSet))
	for k, _ := range store.queueSet {
		keys = append(keys, k)
	}
	store.mu.Unlock()

	sort.Strings(keys)
	for _, k := range keys {
		x(store.queueSet[k])
	}
}

func (store *rocksStore) init() error {
	ro := queueReadOptions(false)
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := store.db.NewIteratorCF(ro, store.queues)
	defer it.Close()
	it.SeekToFirst()

	cur := ""

	for ; it.Valid(); it.Next() {
		if it.Err() != nil {
			return it.Err()
		}
		k := it.Key()
		key := k.Data()
		for i := 0; i < len(key); i++ {
			if key[i] == uint8(255) {
				name := string(key[0:i])
				if cur != name {
					if cur != "" {
						store.GetQueue(name)
					}
					cur = name
				}
				break
			}
		}
		k.Free()
	}
	if cur != "" {
		_, err := store.GetQueue(cur)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *rocksStore) GetQueue(name string) (Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("Queue name cannot be blank")
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	q, ok := store.queueSet[name]
	if ok {
		return q, nil
	}
	q = &rocksQueue{
		name:  name,
		size:  -1,
		store: store,
		cf:    store.queues,
		high:  0,
		low:   0,
	}
	err := q.Init()
	if err != nil {
		return nil, err
	}
	store.queueSet[name] = q
	return q, nil
}

func (store *rocksStore) Close() error {
	util.Info("Stopping storage")
	store.dead.Close()
	store.retries.Close()
	store.working.Close()
	store.scheduled.Close()
	store.defalt.Destroy()
	store.queues.Destroy()
	store.db.Close()
	return nil
}

func (store *rocksStore) Retries() SortedSet {
	return store.retries
}

func (store *rocksStore) Scheduled() SortedSet {
	return store.scheduled
}

func (store *rocksStore) Working() SortedSet {
	return store.working
}

func (store *rocksStore) Dead() SortedSet {
	return store.dead
}
