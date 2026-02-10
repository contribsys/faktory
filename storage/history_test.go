package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	withRedis(t, "history", func(t *testing.T, store Store) {
		bg := context.Background()
		store.Flush(bg)
		var err error

		for i := range 10000 {
			if i%100 == 99 {
				err = store.Failure(bg)
				assert.NoError(t, err)
			} else {
				err = store.Success(bg)
				assert.NoError(t, err)
			}
		}

		assert.EqualValues(t, 10000, store.TotalProcessed(bg))
		assert.EqualValues(t, 100, store.TotalFailures(bg))

		err = store.Failure(bg)
		assert.NoError(t, err)
		err = store.Success(bg)
		assert.NoError(t, err)

		assert.EqualValues(t, 10002, store.TotalProcessed(bg))
		assert.EqualValues(t, 101, store.TotalFailures(bg))

		hash := map[string][2]uint64{}
		err = store.History(bg, 3, func(day string, p, f uint64) {
			hash[day] = [2]uint64{p, f}
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(hash))

		daystr := time.Now().Format("2006-01-02")
		counts := hash[daystr]
		assert.NotNil(t, counts)
		assert.EqualValues(t, 10002, counts[0]) // false alarms, buggy G602 in gosec
		assert.EqualValues(t, 101, counts[1])   // https://github.com/golangci/golangci-lint/issues/6334
	})
}
