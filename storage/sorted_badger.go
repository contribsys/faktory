package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger"
)

type bSortedSet struct {
	name   string
	bdb    *badger.DB
	size   int64
	prefix string
}

func newSortedSet(name, prefix string, db *badger.DB) (*bSortedSet, error) {
	set := &bSortedSet{name: name, bdb: db, size: 0, prefix: prefix}
	err := set.init()
	if err != nil {
		return nil, err
	}
	return set, nil
}

func (ts *bSortedSet) Name() string {
	return ts.name
}

func (ts *bSortedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s%s|%s", ts.prefix, tstamp, jid))
	err := ts.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(key, payload)
	})
	if err != nil {
		return err
	}
	atomic.AddInt64(&ts.size, 1)
	return nil
}

func (ts *bSortedSet) Close() {
	// noop
}

func (ts *bSortedSet) Page(start int64, count int64, proc func(int, []byte, []byte) error) error {
	prefixSize := len(ts.prefix)
	b := []byte(ts.prefix)
	return ts.bdb.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Seek(b)

		// skip any before start point
		for i := start; i > 0; i-- {
			if !it.ValidForPrefix(b) {
				return nil
			}
			it.Next()
		}

		index := 0
		for ; it.ValidForPrefix(b); it.Next() {
			if count == 0 {
				break
			}
			k := it.Item().Key()
			v, err := it.Item().Value()
			if err != nil {
				return err
			}
			err = proc(index, k[prefixSize:], v)
			index++
			if err != nil {
				return err
			}
			count -= 1
		}
		return nil
	})
}

func (ts *bSortedSet) Each(proc func(int, []byte, []byte) error) error {
	return ts.Page(0, -1, proc)
}

func (ts *bSortedSet) Get(key []byte) ([]byte, error) {
	var val []byte
	err := ts.bdb.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(fmt.Sprintf("%s%s", ts.prefix, key)))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (ts *bSortedSet) init() error {
	var count int64
	b := []byte(ts.prefix)

	err := ts.bdb.View(func(tx *badger.Txn) error {
		initOpts := badger.DefaultIteratorOptions
		initOpts.PrefetchValues = false
		it := tx.NewIterator(initOpts)
		defer it.Close()
		it.Seek(b)

		for ; it.ValidForPrefix(b); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return err
	}
	atomic.StoreInt64(&ts.size, count)
	return nil
}

func (ts *bSortedSet) Size() int64 {
	return atomic.LoadInt64(&ts.size)
}

func (ts *bSortedSet) Remove(key []byte) error {
	err := ts.bdb.Update(func(tx *badger.Txn) error {
		return tx.Delete([]byte(fmt.Sprintf("%s%s", ts.prefix, key)))
	})
	if err != nil {
		return err
	}
	atomic.AddInt64(&ts.size, -1)
	return nil
}

func (ts *bSortedSet) RemoveElement(tstamp string, jid string) error {
	return ts.Remove([]byte(fmt.Sprintf("%s|%s", tstamp, jid)))
}

func (ts *bSortedSet) RemoveBefore(tstamp string) ([][]byte, error) {
	last := []byte(ts.prefix + tstamp + "|")
	b := []byte(ts.prefix)
	results := [][]byte{}
	count := int64(0)

	err := ts.bdb.Update(func(tx *badger.Txn) error {
		backwards := badger.DefaultIteratorOptions
		backwards.Reverse = true
		it := tx.NewIterator(backwards)
		defer it.Close()
		it.Seek(last)

		for ; it.ValidForPrefix(b); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			results = append(results, val)
			err = tx.Delete(clone(it.Item().Key()))
			if err != nil {
				return err
			}
			count++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&ts.size, -count)

	// reverse results since we iterated backwards
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	return results, nil
}

func (ts *bSortedSet) MoveTo(ots SortedSet, tstamp string, jid string, mutator func(value []byte) (string, []byte, error)) error {
	other := ots.(*bSortedSet)
	key := []byte(fmt.Sprintf("%s%s|%s", ts.prefix, tstamp, jid))

	err := ts.bdb.Update(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		newtstamp, payload, err := mutator(val)
		if err != nil {
			return err
		}
		newkey := []byte(fmt.Sprintf("%s%s|%s", other.prefix, newtstamp, jid))
		err = tx.Set(newkey, payload)
		if err != nil {
			return err
		}
		err = tx.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	atomic.AddInt64(&ts.size, -1)
	atomic.AddInt64(&other.size, 1)
	return nil
}

func clone(s []byte) []byte {
	c := make([]byte, len(s))
	copy(c, s)
	return c
}

func (ts *bSortedSet) Clear() (int64, error) {
	count := int64(0)

	err := ts.bdb.Update(func(tx *badger.Txn) error {
		b := []byte(ts.prefix)
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(b)

		for ; it.ValidForPrefix(b); it.Next() {
			count++
			err := tx.Delete(clone(it.Item().Key()))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	atomic.AddInt64(&ts.size, -count)
	return count, nil
}

func (ts *bSortedSet) EachCount() int {
	var count int

	ts.Each(func(idx int, k, v []byte) error {
		count++
		return nil
	})
	return count
}
