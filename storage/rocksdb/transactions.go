package rocksdb

import (
	"encoding/json"
	"sync/atomic"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage/types"
	"github.com/contribsys/gorocksdb"
)

type Transaction struct {
	batch     *gorocksdb.WriteBatch
	onSuccess func()
}

func (store *rocksStore) newTransaction() *Transaction {
	return &Transaction{
		batch: gorocksdb.NewWriteBatch(),
	}
}

// Transactional operations

// Enqueue all moves all jobs within the given set into the
// queues associated with those jobs.
func (store *rocksStore) EnqueueAll(set types.SortedSet) error {
	return set.Each(func(idx int, key []byte, data []byte) error {
		return store.EnqueueFrom(set, key)
	})
}

func (store *rocksStore) EnqueueFrom(set types.SortedSet, key []byte) error {
	return store.RunTransaction(func(xa *Transaction) error {
		ss := set.(*rocksSortedSet)

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
		q := queue.(*rocksQueue)
		k := q.nextkey(job.Priority)
		v := data
		xa.batch.PutCF(q.cf, k, v)
		xa.batch.DeleteCF(ss.cf, key)
		xa.onSuccess = func() {
			atomic.AddUint64(&q.size, 1)
			atomic.AddUint64(&ss.size, ^uint64(0))
		}
		return nil
	})
}

func (store *rocksStore) RunTransaction(fn func(xa *Transaction) error) error {
	xa := store.newTransaction()
	defer xa.batch.Destroy()

	err := fn(xa)
	if err != nil {
		return err
	}

	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err = store.db.Write(wo, xa.batch)
	if err != nil {
		return err
	}
	if xa.onSuccess != nil {
		xa.onSuccess()
	}
	return nil
}
