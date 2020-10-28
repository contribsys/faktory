package manager

import (
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	withRedis(t, "retry", func(t *testing.T, store storage.Store) {

		t.Run("fail", func(t *testing.T) {
			store.Flush()
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			job.Retry = 1

			lease := &simpleLease{job: job}

			err := m.reserve("workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size())
			assert.EqualValues(t, 0, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())
			assert.False(t, lease.released)

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 1, store.Retries().Size())
			assert.EqualValues(t, 1, store.TotalProcessed())
			assert.EqualValues(t, 1, store.TotalFailures())
			assert.True(t, lease.released)

			// retry job
			err = m.reserve("workerId", lease)

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
			assert.EqualValues(t, 2, store.TotalProcessed())
			assert.EqualValues(t, 2, store.TotalFailures())
		})

		t.Run("FailOneShotJob", func(t *testing.T) {
			store.Flush()
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			job.Retry = 0

			lease := &simpleLease{job: job}
			err := m.reserve("workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size())
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size())
			assert.EqualValues(t, 0, store.TotalProcessed())
			assert.EqualValues(t, 0, store.TotalFailures())

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 0, store.Retries().Size())
			assert.EqualValues(t, 0, store.Dead().Size())
			assert.EqualValues(t, 1, store.TotalProcessed())
			assert.EqualValues(t, 1, store.TotalFailures())
		})

		t.Run("FailWithInvalidFailPayload", func(t *testing.T) {
			store.Flush()
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
