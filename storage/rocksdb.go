package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"regexp"

	"github.com/contribsys/faktory/storage/brodal"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

type rocksStore struct {
	Name      string
	db        *gorocksdb.DB
	opts      *gorocksdb.Options
	retries   *rocksSortedSet
	scheduled *rocksSortedSet
	working   *rocksSortedSet
	dead      *rocksSortedSet
	clients   *rocksSortedSet
	defalt    *gorocksdb.ColumnFamilyHandle
	queues    *gorocksdb.ColumnFamilyHandle
	stats     *gorocksdb.ColumnFamilyHandle
	queueSet  map[string]*rocksQueue
	mu        sync.Mutex
	history   *processingHistory
}

func DefaultOptions() *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.IncreaseParallelism(2)
	// Try to minimize the number of jobs we'll write to disk.
	// Ideally jobs are processed in-memory, before they need to be
	// flushed to disk.  Defaults to 64MB which is likely more than we need.
	opts.SetWriteBufferSize(2 * 1024 * 1024)
	// default is 6 hrs, set to 1 hr, recovers disk space quicker
	opts.SetDeleteObsoleteFilesPeriodMicros(1000000 * 3600)
	opts.SetKeepLogFileNum(10)
	// these are used in OptimizeForSmallDb()
	// since queues are usually empty, we optimize for a smaller (<1GB) dataset.
	opts.SetMaxFileOpeningThreads(2)
	opts.SetMaxOpenFiles(5000)
	return opts
}

var registerMutex sync.Mutex

func OpenRocks(path string) (Store, error) {

	util.Infof("Initializing storage at %s", path)
	util.Debugf("Using RocksDB v%s", gorocksdb.RocksDBVersion())

	err := os.MkdirAll(path, os.ModeDir|0755)
	if err != nil {
		return nil, err
	}
	opts := DefaultOptions()
	sopts := gorocksdb.NewDefaultOptions()

	// the global registration function in gorocksdb, registerMergeOperator seems to be racy
	registerMutex.Lock()
	sopts.SetMergeOperator(&int64CounterMerge{})
	registerMutex.Unlock()

	db, handles, err := gorocksdb.OpenDbColumnFamilies(opts, path,
		[]string{"scheduled", "retries", "working", "dead", "clients", "default", "queues", "stats"},
		[]*gorocksdb.Options{opts, opts, opts, opts, opts, opts, opts, sopts})
	if err != nil {
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	rs := &rocksStore{
		Name:      path,
		db:        db,
		opts:      opts,
		scheduled: (&rocksSortedSet{name: "scheduled", db: db, cf: handles[0], ro: ro, wo: wo, size: 0}).init(),
		retries:   (&rocksSortedSet{name: "retries", db: db, cf: handles[1], ro: ro, wo: wo, size: 0}).init(),
		working:   (&rocksSortedSet{name: "working", db: db, cf: handles[2], ro: ro, wo: wo, size: 0}).init(),
		dead:      (&rocksSortedSet{name: "dead", db: db, cf: handles[3], ro: ro, wo: wo, size: 0}).init(),
		clients:   (&rocksSortedSet{name: "clients", db: db, cf: handles[4], ro: ro, wo: wo, size: 0}).init(),
		defalt:    handles[5],
		queues:    handles[6],
		stats:     handles[7],
		queueSet:  make(map[string]*rocksQueue),
		mu:        sync.Mutex{},
		history:   &processingHistory{},
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
		"name":  store.db.Name(),
	}
}

func (store *rocksStore) Processed() int64 {
	return atomic.LoadInt64(&store.history.TotalProcessed)
}

func (store *rocksStore) Failures() int64 {
	return atomic.LoadInt64(&store.history.TotalFailures)
}

// queues are iterated in sorted, lexigraphical order
func (store *rocksStore) EachQueue(x func(Queue)) {
	store.mu.Lock()
	keys := make([]string, 0, len(store.queueSet))
	for k := range store.queueSet {
		keys = append(keys, k)
	}
	store.mu.Unlock()

	sort.Strings(keys)
	for _, k := range keys {
		x(store.queueSet[k])
	}
}

func (store *rocksStore) Flush() error {
	// This is a very slow implementation.  Are there lower-level
	// RocksDB operations we can use to bulk-delete everything
	// in a column family or database?
	store.mu.Lock()
	defer store.mu.Unlock()

	var err error
	keys := make([]string, 0, len(store.queueSet))
	for k, q := range store.queueSet {
		keys = append(keys, k)
		_, err = q.Clear()
		if err != nil {
			return err
		}
	}
	for _, k := range keys {
		delete(store.queueSet, k)
	}

	_, err = store.retries.Clear()
	if err != nil {
		return err
	}
	_, err = store.scheduled.Clear()
	if err != nil {
		return err
	}
	_, err = store.dead.Clear()
	if err != nil {
		return err
	}
	_, err = store.working.Clear()
	if err != nil {
		return err
	}
	_, err = store.clients.Clear()
	// flush doesn't clear the stats or default space
	return err
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

	ro = gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	value, err := store.db.GetBytesCF(ro, store.stats, []byte("Processed"))
	if err != nil {
		return err
	}
	if value != nil {
		store.history.TotalProcessed, _ = binary.Varint(value)
	}

	value, err = store.db.GetBytesCF(ro, store.stats, []byte("Failures"))
	if err != nil {
		return err
	}
	if value != nil {
		store.history.TotalFailures, _ = binary.Varint(value)
	}
	return nil
}

var (
	ValidQueueName = regexp.MustCompile(`\A[a-zA-Z0-9._-]+\z`)
)

func (store *rocksStore) GetQueue(name string) (Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("queue name cannot be blank")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	q, ok := store.queueSet[name]
	if ok {
		return q, nil
	}

	if !ValidQueueName.MatchString(name) {
		return nil, fmt.Errorf("queue names must match %v", ValidQueueName)
	}

	q = &rocksQueue{
		name:  name,
		size:  0,
		store: store,
		cf:    store.queues,
		maxsz: DefaultMaxSize,

		pointers:        make(map[uint8]*queuePointer),
		orderedPointers: brodal.NewHeap(),
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
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, q := range store.queueSet {
		q.Close()
	}

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
