package storage

import "github.com/mperham/gorocksdb"

type RocksQueue struct {
	Name  string
	size  int64
	store *RocksStore
	cf    *gorocksdb.ColumnFamilyHandle
}

func (q *RocksQueue) Size() int64 {
	return q.size
}

func (q *RocksQueue) Push(jid string, payload []byte) error {
	return nil
}

func (q *RocksQueue) Pop() (string, []byte, error) {
	return "", nil, nil
}
