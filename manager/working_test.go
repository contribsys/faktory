package manager

import (
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadWorkingSet(t *testing.T) {
	withRedis(t, "working", func(t *testing.T, store storage.Store) {
		t.Run("LoadWorkingSet", func(t *testing.T) {
			store.Flush()
			m := NewManager(store).(*manager)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size())
			assert.EqualValues(t, 0, m.WorkingCount())

			err := m.reserve("workerId", job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())

			m2 := NewManager(store).(*manager)
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m2.WorkingCount())
		})

		t.Run("ManagerReserve", func(t *testing.T) {
			store.Flush()
			m := NewManager(store).(*manager)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size())
			assert.EqualValues(t, 0, m.WorkingCount())

			err := m.reserve("workerId", job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())
		})

		t.Run("ReserveWithInvalidTimeout", func(t *testing.T) {
			store.Flush()
			m := NewManager(store).(*manager)

			timeouts := []int{0, 20, 50, 59, 86401, 100000}
			for _, timeout := range timeouts {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.ReserveFor = timeout
				assert.EqualValues(t, 0, store.Working().Size())

				// doesn't return an error but resets to default timeout
				err := m.reserve("workerId", job)

				assert.NoError(t, err)
				assert.EqualValues(t, 1, store.Working().Size())
				store.Working().Clear()
			}
		})

		t.Run("ManagerAcknowledge", func(t *testing.T) {
			store.Flush()
			m := NewManager(store).(*manager)

			job, err := m.Acknowledge("")
			assert.NoError(t, err)
			assert.Nil(t, job)

			job = client.NewJob("AckJob", 1, 2, 3)
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 0, store.Working().Size())
			assert.EqualValues(t, 0, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())

			err = m.reserve("workerId", job)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())

			assert.EqualValues(t, 1, m.BusyCount("workerId"))
			assert.EqualValues(t, 0, m.BusyCount("fakeId"))

			aJob, err := m.Acknowledge(job.Jid)
			assert.NoError(t, err)
			assert.Equal(t, job.Jid, aJob.Jid)
			assert.EqualValues(t, 1, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())
			assert.EqualValues(t, 0, m.BusyCount("workerId"))

			aJob, err = m.Acknowledge(job.Jid)
			assert.NoError(t, err)
			assert.Nil(t, aJob)
			assert.EqualValues(t, 1, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())
		})

		t.Run("ManagerReapExpiredJobs", func(t *testing.T) {
			store.Flush()
			m := NewManager(store).(*manager)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 0, store.Working().Size())
			assert.EqualValues(t, 0, m.WorkingCount())

			err = m.reserve("workerId", job)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())

			exp := time.Now().Add(time.Duration(10) * time.Second)
			count, err := m.ReapExpiredJobs(util.Thens(exp))
			assert.NoError(t, err)
			assert.Equal(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size())

			exp = time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err = m.ReapExpiredJobs(util.Thens(exp))
			assert.NoError(t, err)
			assert.Equal(t, 1, count)
			assert.EqualValues(t, 1, store.Retries().Size())
		})
	})
}
