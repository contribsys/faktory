package storage

import (
	"bytes"
	"fmt"
	"time"

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

func (ts *TimedSet) AddElement(tstamp time.Time, jid string, payload []byte) error {
	key := fmt.Sprintf("%.10d|%s", tstamp.Unix(), jid)

	return ts.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Put([]byte(key), payload)
	})
}

func (ts *TimedSet) RemoveBefore(tstamp time.Time) ([][]byte, error) {
	key := fmt.Sprintf("%.10d|", tstamp.Unix())
	prefix := []byte(key)

	results := [][]byte{}
	count := 0

	err := ts.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		c := b.Cursor()

		for k, v := c.Seek([]byte("0")); k != nil && bytes.Compare(k, prefix) <= 0; k, v = c.Next() {
			cp := make([]byte, len(v))
			copy(cp, v)
			results = append(results, cp)
			count += 1
			err := b.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (ts *TimedSet) view(f func(*bolt.Bucket) error) {
	ts.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(ts.Name))
		return f(b)
	})
}
