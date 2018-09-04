package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	withRedis(t, "history", func(t *testing.T, store Store) {
		store.Flush()
		for i := 0; i < 10000; i++ {
			if i%100 == 99 {
				store.Failure()
			} else {
				store.Success()
			}
		}

		assert.EqualValues(t, 10000, store.TotalProcessed())
		assert.EqualValues(t, 100, store.TotalFailures())

		store.Failure()
		store.Success()

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
	})
}
