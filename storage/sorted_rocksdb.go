package storage

import (
	"fmt"
	"strings"

	"github.com/mperham/gorocksdb"
)

/*
 * Retries and Scheduled jobs are held in a bucket, sorted based on their timestamp.
 */
type RocksSortedSet struct {
	Name string
	db   *gorocksdb.DB
	cf   *gorocksdb.ColumnFamilyHandle
	ro   *gorocksdb.ReadOptions
	wo   *gorocksdb.WriteOptions
}

func (ts *RocksSortedSet) AddElement(tstamp string, jid string, payload []byte) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))
	return ts.db.PutCF(ts.wo, ts.cf, key, payload)
}

func (ts *RocksSortedSet) Close() {
	ts.cf.Destroy()
}

func (ts *RocksSortedSet) EachElement(proc func(string, string, []byte) error) error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	it := ts.db.NewIteratorCF(ro, ts.cf)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if err := it.Err(); err != nil {
			return err
		}
		k := it.Key()
		v := it.Value()
		key := k.Data()
		strs := strings.Split(string(key), "|")
		payload := v.Data()
		err := proc(strs[0], strs[1], payload)
		k.Free()
		v.Free()
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *RocksSortedSet) Size() int {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	it := ts.db.NewIteratorCF(ro, ts.cf)
	defer it.Close()

	count := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		count += 1
	}
	if err := it.Err(); err != nil {
		panic(fmt.Sprintf("%s size: %s", ts.Name, err.Error()))
	}
	return count
}

func (ts *RocksSortedSet) RemoveElement(tstamp string, jid string) error {
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))
	return ts.db.DeleteCF(ts.wo, ts.cf, key)
}

func (ts *RocksSortedSet) RemoveBefore(tstamp string) ([][]byte, error) {
	prefix := []byte(tstamp + "|")
	results := [][]byte{}
	count := int64(0)

	// TODO does Rocks have range deletes?
	wb := gorocksdb.NewWriteBatch()
	it := ts.db.NewIteratorCF(ts.ro, ts.cf)
	defer it.Close()
	it.SeekForPrev(prefix)
	for ; it.Valid(); it.Prev() {
		k := it.Key()
		v := it.Value()
		cp := make([]byte, v.Size())
		copy(cp, v.Data())
		results = append(results, cp)
		wb.DeleteCF(ts.cf, k.Data())
		k.Free()
		v.Free()
		count += 1
	}
	if count > 0 {
		err := ts.db.Write(ts.wo, wb)
		if err != nil {
			return nil, err
		}
	}

	// reverse results since we iterated backwards
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	return results, nil
}
func (ts *RocksSortedSet) MoveTo(ots SortedSet, tstamp string, jid string, mutator func(value []byte) (string, []byte, error)) error {
	other := ots.(*RocksSortedSet)
	key := []byte(fmt.Sprintf("%s|%s", tstamp, jid))

	slice, err := ts.db.GetCF(ts.ro, ts.cf, key)
	if err != nil {
		return err
	}
	defer slice.Free()

	data := slice.Data()
	if len(data) == 0 {
		return fmt.Errorf("Element not found in %s: %s", ts.Name, jid)
	}

	newtstamp, payload, err := mutator(data)
	if err != nil {
		return err
	}
	newkey := []byte(fmt.Sprintf("%s|%s", newtstamp, jid))

	wb := gorocksdb.NewWriteBatch()
	wb.DeleteCF(ts.cf, key)
	wb.PutCF(other.cf, newkey, payload)
	return ts.db.Write(ts.wo, wb)
}
