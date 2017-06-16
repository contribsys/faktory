package storage

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type TimedSet struct {
	Name string
	db   *bolt.DB
}

func (ts *TimedSet) view(f func(*bolt.Bucket) error) {
	ts.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(ts.Name))
		return f(b)
	})
}

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

func (ts *TimedSet) Add(sec int64, jid string, payload []byte) error {
	timestamp := time.Unix(sec, 0)
	key := []byte(fmt.Sprintf(timestamp.Format(time.RFC3339)+"-%s", jid))

	return ts.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Put(key, payload)
	})
}
