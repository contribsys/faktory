package manager

import (
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestManagerFail(t *testing.T) {
	t.Run("Fail", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store).(*manager)

		job := client.NewJob("ManagerPush", 1, 2, 3)
		job.Retry = 1

		err := m.reserve("workerId", job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, store.Working().Size())
		assert.EqualValues(t, 1, m.WorkingCount())
		assert.NotNil(t, m.workingMap[job.Jid])
		assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
		assert.EqualValues(t, 0, store.Retries().Size())
		assert.EqualValues(t, 0, store.Processed())
		assert.EqualValues(t, 0, store.Failures())

		fail := failure(job.Jid, "uh no", "SomeError", nil)
		err = m.Fail(fail)

		assert.NoError(t, err)
		assert.Nil(t, m.workingMap[job.Jid])
		assert.EqualValues(t, 1, store.Retries().Size())
		assert.EqualValues(t, 1, store.Processed())
		assert.EqualValues(t, 1, store.Failures())

		// retry job
		err = m.reserve("workerId", job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, store.Working().Size())
		assert.EqualValues(t, 1, m.WorkingCount())
		assert.NotNil(t, m.workingMap[job.Jid])
		assert.NotNil(t, m.workingMap[job.Jid].Job.Failure)
		assert.EqualValues(t, 1, store.Retries().Size())
		assert.EqualValues(t, 0, store.Dead().Size())

		fail = failure(job.Jid, "uh no again", "YetAnotherError", nil)
		err = m.Fail(fail)

		assert.NoError(t, err)
		assert.Nil(t, m.workingMap[job.Jid])
		assert.EqualValues(t, 1, store.Retries().Size())
		assert.EqualValues(t, 1, store.Dead().Size())
		assert.EqualValues(t, 2, store.Processed())
		assert.EqualValues(t, 2, store.Failures())
	})

	t.Run("FailOneShotJob", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store).(*manager)

		job := client.NewJob("ManagerPush", 1, 2, 3)
		job.Retry = 0

		err := m.reserve("workerId", job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, store.Working().Size())
		assert.EqualValues(t, 1, m.WorkingCount())
		assert.NotNil(t, m.workingMap[job.Jid])
		assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
		assert.EqualValues(t, 0, store.Retries().Size())
		assert.EqualValues(t, 0, store.Processed())
		assert.EqualValues(t, 0, store.Failures())

		fail := failure(job.Jid, "uh no", "SomeError", nil)
		err = m.Fail(fail)

		assert.NoError(t, err)
		assert.Nil(t, m.workingMap[job.Jid])
		assert.EqualValues(t, 0, store.Retries().Size())
		assert.EqualValues(t, 0, store.Dead().Size())
		assert.EqualValues(t, 1, store.Processed())
		assert.EqualValues(t, 1, store.Failures())
	})

	t.Run("FailWithInvalidFailPayload", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		err := m.Fail(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "No failure")

		err = m.Fail(&FailPayload{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Missing JID")

		err = m.Fail(&FailPayload{Jid: "1238123123"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func failure(jid, msg, errtype string, bt []string) *FailPayload {
	var f FailPayload
	f.Jid = jid
	f.ErrorMessage = msg
	f.ErrorType = errtype
	f.Backtrace = bt
	return &f
}
