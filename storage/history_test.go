package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/merge.db")
	db, err := Open("redis", "/tmp/merge.db")
	assert.NoError(t, err)

	store := db.(*redisStore)
	for i := 0; i < 10000; i++ {
		if i%100 == 99 {
			store.Failure()
		} else {
			store.Success()
		}
	}

	assert.EqualValues(t, 10000, store.TotalProcessed())
	assert.EqualValues(t, 100, store.TotalFailures())

	value, err := store.client.IncrBy("Processed", 0).Result()
	assert.NoError(t, err)
	assert.EqualValues(t, 10000, value)

	value, err = store.client.IncrBy("Failures", 0).Result()
	assert.NoError(t, err)
	assert.EqualValues(t, 100, value)

	store.Failure()
	store.Success()
	db.Close()

	db, err = Open("redis", "/tmp/merge.db")
	assert.NoError(t, err)
	defer db.Close()

	store = db.(*redisStore)
	assert.EqualValues(t, 10002, store.TotalProcessed())
	assert.EqualValues(t, 101, store.TotalFailures())

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
