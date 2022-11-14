package manager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestScheduler(t *testing.T) {
	withRedis(t, "scheduler", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("Purge", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			assert.EqualValues(t, 0, store.Dead().Size(bg))

			job := client.NewJob("DeadJob", 1, 2, 3)
			expiry := util.Thens(time.Now())
			addJob(bg, t, store.Dead(), expiry, job)

			assert.EqualValues(t, 1, store.Dead().Size(bg))

			count, err := m.Purge(bg, time.Now())
			assert.NoError(t, err)

			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 0, store.Dead().Size(bg))

			job = client.NewJob("DeadJob1", 1, 2, 3)
			expiry = util.Thens(time.Now())
			addJob(bg, t, store.Dead(), expiry, job)

			job = client.NewJob("DeadJob2", 1, 2, 3)
			expiry = util.Thens(time.Now().Add(time.Duration(5) * time.Minute))
			addJob(bg, t, store.Dead(), expiry, job)

			assert.EqualValues(t, 2, store.Dead().Size(bg))

			count, err = m.Purge(bg, time.Now())
			assert.NoError(t, err)

			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, store.Dead().Size(bg))
		})

		t.Run("EnqueueScheduledJobs", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))

			expiry := util.Thens(time.Now())
			addJob(bg, t, store.Scheduled(), expiry, job)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Scheduled().Size(bg))

			count, err := m.EnqueueScheduledJobs(bg, time.Now())
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))
		})

		t.Run("EnqueueScheduledMultipleJobs", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ScheduledJob1", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))

			expiry := util.Thens(time.Now())
			addJob(bg, t, store.Scheduled(), expiry, job)

			job = client.NewJob("ScheduledJob2", 1, 2, 3)
			expiry = util.Thens(time.Now().Add(time.Duration(5) * time.Minute))
			addJob(bg, t, store.Scheduled(), expiry, job)

			job = client.NewJob("ScheduledJob3", 1, 2, 3)
			expiry = util.Thens(time.Now().Add(time.Duration(8) * time.Minute))
			addJob(bg, t, store.Scheduled(), expiry, job)

			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 3, store.Scheduled().Size(bg))

			count, err := m.EnqueueScheduledJobs(bg, time.Now())
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.EqualValues(t, 2, store.Scheduled().Size(bg))
		})

		t.Run("RetryJobs", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("FailedJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			expiry := util.Thens(time.Now())
			addJob(bg, t, store.Retries(), expiry, job)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Retries().Size(bg))

			count, err := m.RetryJobs(bg, time.Now())
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.EqualValues(t, 0, store.Retries().Size(bg))
		})
	})
}

func addJob(ctx context.Context, t *testing.T, set storage.SortedSet, timestamp string, job *client.Job) {
	data, err := json.Marshal(job)
	assert.NoError(t, err)

	err = set.AddElement(ctx, timestamp, job.Jid, data)
	assert.NoError(t, err)
}
