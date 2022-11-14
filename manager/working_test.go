package manager

import (
	"context"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadWorkingSet(t *testing.T) {
	withRedis(t, "working", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		t.Run("LoadWorkingSet", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			m2 := newManager(store)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m2.WorkingCount())
		})

		t.Run("ManagerReserve", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
		})

		t.Run("ReserveWithInvalidTimeout", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			timeouts := []int{0, 20, 50, 59, 86401, 100000}
			for _, timeout := range timeouts {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.ReserveFor = timeout
				assert.EqualValues(t, 0, store.Working().Size(bg))

				// doesn't return an error but resets to default timeout
				lease := &simpleLease{job: job}
				err := m.reserve(bg, "workerId", lease)

				assert.NoError(t, err)
				assert.EqualValues(t, 1, store.Working().Size(bg))
				err = store.Working().Clear(bg)
				assert.NoError(t, err)
			}
		})

		t.Run("ManagerAcknowledge", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job, err := m.Acknowledge(bg, "")
			assert.NoError(t, err)
			assert.Nil(t, job)

			job = client.NewJob("AckJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.False(t, lease.released)

			assert.EqualValues(t, 1, m.BusyCount("workerId"))
			assert.EqualValues(t, 0, m.BusyCount("fakeId"))

			aJob, err := m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Equal(t, job.Jid, aJob.Jid)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.EqualValues(t, 0, m.BusyCount("workerId"))
			assert.True(t, lease.released)

			aJob, err = m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Nil(t, aJob)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
		})

		t.Run("ManagerReapExpiredJobs", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			exp := time.Now().Add(time.Duration(10) * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			err = m.ExtendReservation(bg, "nosuch", time.Now().Add(50*time.Hour))
			assert.NoError(t, err)

			util.LogInfo = true
			util.LogDebug = true
			util.Infof("Extending %s", job.Jid)
			err = m.ExtendReservation(bg, job.Jid, time.Now().Add(50*time.Hour))
			assert.NoError(t, err)

			exp = time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			exp = time.Now().Add(51 * time.Hour)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
		})
	})
}
