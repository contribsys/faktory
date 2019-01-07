package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicSortedOps(t *testing.T) {
	withRedis(t, "sorted", func(t *testing.T, store Store) {

		t.Run("large set", func(t *testing.T) {
			sset := store.Retries()
			sset.Clear()
			for i := 0; i < 55; i++ {
				job := client.NewJob("OtherType", 1, 2, 3)
				job.At = util.Nows()
				err := sset.Add(job)
				assert.NoError(t, err)
			}
			assert.EqualValues(t, 55, sset.Size())

			count := 0
			err := sset.Each(func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				assert.NotNil(t, j)
				count += 1
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 55, count)
			sset.Clear()
		})

		t.Run("junk data", func(t *testing.T) {
			sset := store.Retries()
			assert.EqualValues(t, 0, sset.Size())

			time := util.Nows()
			jid, data := fakeJob()
			err := sset.AddElement(time, jid, data)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			key := fmt.Sprintf("%s|%s", time, jid)
			entry, err := sset.Get([]byte(key))
			assert.NoError(t, err)
			assert.NotNil(t, entry)
			job, err := entry.Job()
			assert.NoError(t, err)
			assert.Equal(t, jid, job.Jid)

			// add a second job with exact same time to handle edge case of
			// sorted set entries with same score.
			newjid, payload := fakeJob()
			err = sset.AddElement(time, newjid, payload)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size())

			newkey := fmt.Sprintf("%s|%s", time, newjid)
			entry, err = sset.Get([]byte(newkey))
			assert.NoError(t, err)
			assert.Equal(t, payload, entry.Value())

			ok, err := sset.Remove([]byte(newkey))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())
			assert.True(t, ok)

			ok, err = sset.RemoveElement(time, jid)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size())
			assert.True(t, ok)

			err = sset.AddElement(time, newjid, payload)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			assert.Equal(t, sset.Name(), "retries")
			assert.NoError(t, sset.Clear())
			assert.EqualValues(t, 0, sset.Size())
		})

		t.Run("good data", func(t *testing.T) {
			sset := store.Scheduled()
			job := client.NewJob("SomeType", 1, 2, 3)

			assert.EqualValues(t, 0, sset.Size())
			err := sset.Add(job)
			assert.Error(t, err)

			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			job = client.NewJob("OtherType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size())

			expectedTypes := []string{"SomeType", "OtherType"}
			actualTypes := []string{}

			err = sset.Each(func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				actualTypes = append(actualTypes, j.Type)
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, expectedTypes, actualTypes)

			var jkey []byte
			err = sset.Each(func(idx int, entry SortedEntry) error {
				k, err := entry.Key()
				assert.NoError(t, err)
				jkey = k
				return nil
			})
			assert.NoError(t, err)

			q, err := store.GetQueue("default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 2, sset.Size())

			err = store.EnqueueFrom(sset, jkey)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())
			assert.EqualValues(t, 1, sset.Size())

			err = store.EnqueueAll(sset)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size())
			assert.EqualValues(t, 0, sset.Size())

			job = client.NewJob("CronType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size())

			err = sset.Each(func(idx int, entry SortedEntry) error {
				k, err := entry.Key()
				assert.NoError(t, err)
				jkey = k
				return nil
			})
			assert.NoError(t, err)

			entry, err := sset.Get(jkey)
			assert.NoError(t, err)

			expiry := time.Now().Add(180 * 24 * time.Hour)

			assert.EqualValues(t, 1, sset.Size())
			assert.EqualValues(t, 0, store.Dead().Size())
			err = sset.MoveTo(store.Dead(), entry, expiry)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size())
			assert.EqualValues(t, 1, store.Dead().Size())

		})
	})
}
