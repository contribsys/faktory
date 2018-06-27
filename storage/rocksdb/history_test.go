package rocksdb

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/contribsys/gorocksdb"
	"github.com/stretchr/testify/assert"
)

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/merge.db")
	db, err := OpenRocks("/tmp/merge.db")
	assert.NoError(t, err)

	store := db.(*rocksStore)
	for i := 0; i < 10000; i++ {
		if i%100 == 99 {
			store.Failure()
		} else {
			store.Success()
		}
	}

	assert.EqualValues(t, 10000, store.history.TotalProcessed)
	assert.EqualValues(t, 100, store.history.TotalFailures)
	//store.db.Flush()

	ro := gorocksdb.NewDefaultReadOptions()
	value, err := store.db.GetBytesCF(ro, store.stats, []byte("Processed"))
	assert.NoError(t, err)
	count, _ := binary.Uvarint(value)
	assert.EqualValues(t, 10000, count)

	value, err = store.db.GetBytesCF(ro, store.stats, []byte("Failures"))
	assert.NoError(t, err)
	count, _ = binary.Uvarint(value)
	assert.EqualValues(t, 100, count)

	store.Failure()
	store.Success()
	db.Close()

	db, err = OpenRocks("/tmp/merge.db")
	assert.NoError(t, err)
	defer db.Close()

	store = db.(*rocksStore)
	assert.EqualValues(t, 10002, store.history.TotalProcessed)
	assert.EqualValues(t, 101, store.history.TotalFailures)

	hash := map[string][2]uint64{}
	store.History(3, func(day string, p, f uint64) {
		hash[day] = [2]uint64{p, f}
	})
	assert.Equal(t, 3, len(hash))

	daystr := time.Now().Format("2006-01-02")
	counts := hash[daystr]
	assert.NotNil(t, counts)
	assert.EqualValues(t, 10002, counts[0])
	assert.EqualValues(t, 101, counts[1])
}
