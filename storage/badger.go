package storage

import (
	"fmt"
	"regexp"
	"sort"
	"sync"

	"github.com/contribsys/faktory/util"
	"github.com/dgraph-io/badger"
)

type bStore struct {
	bdb       *badger.DB
	retries   *bSortedSet
	working   *bSortedSet
	dead      *bSortedSet
	scheduled *bSortedSet
	history   *processingHistory

	mu       sync.Mutex
	queueSet map[string]*bQueue
}

func OpenBadger(path string) (Store, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	work, err := newSortedSet("Working", "w~", db)
	if err != nil {
		return nil, err
	}
	ret, err := newSortedSet("Retries", "r~", db)
	if err != nil {
		return nil, err
	}
	dead, err := newSortedSet("Dead", "d~", db)
	if err != nil {
		return nil, err
	}
	scheduled, err := newSortedSet("Scheduled", "s~", db)
	if err != nil {
		return nil, err
	}

	return &bStore{bdb: db,
		retries:   ret,
		working:   work,
		dead:      dead,
		scheduled: scheduled,
		mu:        sync.Mutex{},
		queueSet:  make(map[string]*bQueue),
		history:   &processingHistory{0, 0},
	}, nil
}

var (
	ValidQueueName = regexp.MustCompile(`\A[a-zA-Z0-9._-]+\z`)
)

func (store *bStore) Label() string {
	return "Badger 1.3.0"
}

func (store *bStore) GetQueue(name string) (Queue, error) {
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

	q, err := newQueue(name, store)
	if err != nil {
		return nil, err
	}
	store.queueSet[name] = q
	return q, nil
}

func (store *bStore) Close() error {
	util.Info("Stopping storage")
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, q := range store.queueSet {
		q.Close()
	}

	//store.dead.Close()
	store.retries.Close()
	store.working.Close()
	//store.scheduled.Close()
	store.bdb.Close()
	return nil
}

func (db *bStore) Retries() SortedSet {
	return db.retries
}
func (db *bStore) Scheduled() SortedSet {
	return db.scheduled
}
func (db *bStore) Working() SortedSet {
	return db.working
}
func (db *bStore) Dead() SortedSet {
	return db.dead
}
func (store *bStore) EachQueue(x func(Queue)) {
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
func (db *bStore) Stats() map[string]string {
	return nil
}

// Clear the database of all job data.
// Equivalent to Redis's FLUSHDB
func (db *bStore) Flush() error {
	return nil
}
