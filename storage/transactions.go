package storage

import (
	"encoding/json"
	"sync/atomic"

	"github.com/mperham/faktory"
	"github.com/mperham/gorocksdb"
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

/*
 * Transactional operations
 *
 * Enqueue all moves all jobs within the given set into the
 * queues associated with those jobs.  We do this 100 jobs
 * per transaction.
 */
func (store *rocksStore) EnqueueAll(set SortedSet) error {
	count := int(set.Size())

	for i := 0; i < count; i++ {
		store.RunTransaction(func(xa *Transaction) error {
			ss := set.(*rocksSortedSet)

			return ss.Page(0, 1, func(idx int, key string, data []byte) error {
				var job faktory.Job
				err := json.Unmarshal(data, &job)
				if err != nil {
					return err
				}
				queue, err := store.GetQueue(job.Queue)
				if err != nil {
					return err
				}
				q := queue.(*rocksQueue)
				k := q.nextkey()
				v := data
				xa.batch.PutCF(q.cf, k, v)
				xa.batch.DeleteCF(ss.cf, []byte(key))
				xa.onSuccess = func() {
					atomic.AddInt64(&q.size, 1)
					atomic.AddInt64(&ss.size, -1)
				}
				return nil
			})
		})
	}
	return nil
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
