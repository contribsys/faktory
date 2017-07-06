package storage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
)

/*
 * Retries and Scheduled jobs are held in a bucket, sorted based on their timestamp.
 */
type BoltSortedSet struct {
	Name string
	db   *bolt.DB
}

func (ts *BoltSortedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	err := ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		err := b.Put(key, payload)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (ts *BoltSortedSet) Size() int {
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

func (ts *BoltSortedSet) EachElement(proc func(string, string, []byte) error) error {
	er := ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			payload := make([]byte, len(v))
			copy(payload, v)
			key := make([]byte, len(k))
			copy(key, k)
			strs := strings.Split(string(key), "|")
			err := proc(strs[0], strs[1], payload)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return er
}

func (ts *BoltSortedSet) RemoveElement(tstamp string, jid string) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	err := ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		return b.Delete(key)
	})
	return err
}

func (ts *BoltSortedSet) RemoveBefore(tstamp string) ([][]byte, error) {
	prefix := []byte(tstamp + "|")
	results := [][]byte{}

	err := ts.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Name))
		c := b.Cursor()
		local := [][]byte{}

		for k, v := c.First(); k != nil && bytes.Compare(k, prefix) <= 0; k, v = c.Next() {
			cp := make([]byte, len(v))
			copy(cp, v)
			local = append(local, cp)
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
func (ts *BoltSortedSet) MoveTo(ots SortedSet, tstamp string, jid string, mutator func(value []byte) (string, []byte, error)) error {
	return nil
}
