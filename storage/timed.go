package storage

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
)

/*
 * Retries and Scheduled jobs are held in a bucket, sorted based on their timestamp.
 */
type TimedSet struct {
	Name string
	db   *bolt.DB
}

/*
 * Warning: Complexity: O(N), call with caution.
 */
func (ts *TimedSet) Size() int {
	count := 0

	ts.view(func(b *bolt.Bucket) error {
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count += 1
		}
		return nil
	})

	return count
}

func (ts *TimedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	return ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Put(key, payload)
	})
}

func (ts *TimedSet) RemoveElement(tstamp string, jid string) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	return ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Delete(key)
	})
}

func (ts *TimedSet) RemoveBefore(tstamp string) ([][]byte, error) {
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

func (ts *TimedSet) view(f func(*bolt.Bucket) error) {
	ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return f(b)
	})
}
