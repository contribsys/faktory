package manager

import (
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadWorkingSet(t *testing.T) {
	t.Parallel()
	store, teardown := setupTest(t)
	defer teardown(t)

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
}

func TestManagerReserve(t *testing.T) {
	t.Run("ReserveJob", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

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
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

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
}

func TestManagerAcknowledge(t *testing.T) {
	t.Parallel()
	store, teardown := setupTest(t)
	defer teardown(t)

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
	assert.EqualValues(t, 0, store.Processed())
	assert.EqualValues(t, 0, store.Failures())

	err = m.reserve("workerId", job)

	assert.NoError(t, err)
	assert.EqualValues(t, 0, q.Size())
	assert.EqualValues(t, 1, store.Working().Size())
	assert.EqualValues(t, 1, m.WorkingCount())
	assert.EqualValues(t, 0, store.Processed())
	assert.EqualValues(t, 0, store.Failures())

	assert.EqualValues(t, 1, m.BusyCount("workerId"))
	assert.EqualValues(t, 0, m.BusyCount("fakeId"))

	aJob, err := m.Acknowledge(job.Jid)
	assert.NoError(t, err)
	assert.Equal(t, job.Jid, aJob.Jid)
	assert.EqualValues(t, 1, store.Processed())
	assert.EqualValues(t, 0, store.Failures())
	assert.EqualValues(t, 0, m.BusyCount("workerId"))

	aJob, err = m.Acknowledge(job.Jid)
	assert.NoError(t, err)
	assert.Nil(t, aJob)
	assert.EqualValues(t, 1, store.Processed())
	assert.EqualValues(t, 0, store.Failures())
}

func TestManagerReapExpiredJobs(t *testing.T) {
	t.Parallel()
	store, teardown := setupTest(t)
	defer teardown(t)

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
}
