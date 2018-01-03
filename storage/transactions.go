package storage

import (
	"encoding/json"
	"sync/atomic"

	"github.com/contribsys/faktory/client"
	"github.com/dgraph-io/badger"
)

// Transactional operations

// Enqueue all moves all jobs within the given set into the
// queues associated with those jobs.
func (store *bStore) EnqueueAll(set SortedSet) error {
	return set.Each(func(idx int, key []byte, data []byte) error {
		return store.EnqueueFrom(set, key)
	})
}

func (store *bStore) EnqueueFrom(set SortedSet, key []byte) error {
	return store.bdb.Update(func(xa *badger.Txn) error {
		ss := set.(*bSortedSet)
		data, err := ss.Get(key)
		if err != nil {
			return err
		}
		if data == nil {
			return nil
		}
		var job client.Job
		err = json.Unmarshal(data, &job)
		if err != nil {
			return err
		}
		queue, err := store.GetQueue(job.Queue)
		if err != nil {
			return err
		}
		q := queue.(*bQueue)
		k := q.nextkey(job.Priority)
		v := data
		err = xa.Set(k, v)
		if err != nil {
			return err
		}
		err = xa.Delete(key)
		if err != nil {
			return err
		}
		return xa.Commit(func(error) {
			if err != nil {
				atomic.AddUint64(&q.size, 1)
				atomic.AddInt64(&ss.size, -1)
			}
		})
	})
}
