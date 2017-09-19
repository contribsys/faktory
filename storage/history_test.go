package storage

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/mperham/gorocksdb"
	"github.com/stretchr/testify/assert"
)

func init() {
	DefaultPath = "/tmp"
	os.Mkdir("/tmp", os.FileMode(os.ModeDir|0755))
}

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/merge.db")
	db, err := Open("rocksdb", "merge.db")
	assert.NoError(t, err)

	store := db.(*rocksStore)
	for i := 0; i < 100000; i++ {
		if i%100 == 99 {
			store.Failure()
		} else {
			store.Success()
		}
	}

	assert.Equal(t, int64(100000), store.history.TotalProcessed)
	assert.Equal(t, int64(1000), store.history.TotalFailures)
	//store.db.Flush()

	ro := gorocksdb.NewDefaultReadOptions()
	value, err := store.db.GetBytesCF(ro, store.stats, []byte("Processed"))
	assert.NoError(t, err)
	count, _ := binary.Varint(value)
	assert.Equal(t, int64(100000), count)

	value, err = store.db.GetBytesCF(ro, store.stats, []byte("Failures"))
	assert.NoError(t, err)
	count, _ = binary.Varint(value)
	assert.Equal(t, int64(1000), count)

	store.Failure()
	store.Success()
	db.Close()

	db, err = Open("rocksdb", "merge.db")
	assert.NoError(t, err)
	defer db.Close()

	store = db.(*rocksStore)
	assert.Equal(t, int64(100002), store.history.TotalProcessed)
	assert.Equal(t, int64(1001), store.history.TotalFailures)
}
