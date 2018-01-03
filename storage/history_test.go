package storage

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/merge.db")
	db, err := Open("badger", "/tmp/merge.db")
	assert.NoError(t, err)

	store := db.(*bStore)
	for i := 0; i < 10000; i++ {
		if i%100 == 99 {
			store.Failure()
		} else {
			store.Success()
		}
	}

	assert.Equal(t, int64(10000), store.history.TotalProcessed)
	assert.Equal(t, int64(100), store.history.TotalFailures)
	//store.db.Flush()

	err = store.bdb.View(func(tx *badger.Txn) error {
		value, err := tx.Get([]byte("z~Processed"))
		assert.NoError(t, err)
		val, err := value.ValueCopy(nil)
		assert.NoError(t, err)
		count, _ := binary.Varint(val)
		assert.Equal(t, int64(10000), count)

		value, err = tx.Get([]byte("z~Failures"))
		assert.NoError(t, err)
		val, err = value.ValueCopy(nil)
		assert.NoError(t, err)
		count, _ = binary.Varint(val)
		assert.Equal(t, int64(100), count)
		return nil
	})
	assert.NoError(t, err)

	store.Failure()
	store.Success()
	db.Close()

	db, err = Open("badger", "/tmp/merge.db")
	assert.NoError(t, err)
	defer db.Close()

	store = db.(*bStore)
	assert.Equal(t, int64(10002), store.history.TotalProcessed)
	assert.Equal(t, int64(101), store.history.TotalFailures)

	hash := map[string][2]int64{}
	store.History(3, func(day string, p, f int64) {
		hash[day] = [2]int64{p, f}
	})
	assert.Equal(t, 3, len(hash))

	daystr := time.Now().Format("2006-01-02")
	counts := hash[daystr]
	assert.NotNil(t, counts)
	assert.Equal(t, int64(10002), counts[0])
	assert.Equal(t, int64(101), counts[1])
}
