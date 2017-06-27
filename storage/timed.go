package storage

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

/*
 * Retries and Scheduled jobs are held in a bucket, sorted based on their timestamp.
 */
type TimedSet struct {
	Name string
	db   *bolt.DB
	q    *list.List
	m    sync.Mutex
}

type kv struct {
	k []byte
	v []byte
}

func newTimedSet(name string, db *bolt.DB) *TimedSet {
	return &TimedSet{Name: name, db: db, q: list.New(), m: sync.Mutex{}}
}

func (ts *TimedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	ts.m.Lock()
	ts.q.PushBack(&kv{key, payload})
	ts.m.Unlock()
	return nil
}

func (ts *TimedSet) flush() error {
	ts.m.Lock()
	defer ts.m.Unlock()

	if ts.q.Front() == nil {
		return nil
	}
	err := ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		for e := ts.q.Front(); e != nil; e = e.Next() {
			kv := e.Value.(*kv)
			err := b.Put(kv.k, kv.v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		ts.q.Init()
	}
	return err
}

func (ts *TimedSet) Size() int {
	ts.flush()

	count := 0
	ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count += 1
		}
		return nil
	})
	return count
}

func (ts *TimedSet) RemoveElement(tstamp string, jid string) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	return ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Delete(key)
	})
}

func (ts *TimedSet) RemoveBefore(tstamp string) ([][]byte, error) {
	ts.flush()
	prefix := []byte(tstamp + "|")
	results := [][]byte{}
	count := 0

	err := ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		c := b.Cursor()
		local := [][]byte{}

		for k, v := c.First(); k != nil && bytes.Compare(k, prefix) <= 0; k, v = c.Next() {
			cp := make([]byte, len(v))
			copy(cp, v)
			local = append(local, cp)
			count += 1
			err := b.Delete(k)
			if err != nil {
				return err
			}
		}
		results = local
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}
