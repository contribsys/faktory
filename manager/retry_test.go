package manager

import (
	"context"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	withRedis(t, "retry", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("fail", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			retries := 1
			job.Retry = &retries

			lease := &simpleLease{job: job}

			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.False(t, lease.released)

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 1, store.TotalFailures(bg))
			assert.True(t, lease.released)

			// retry job
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.NotNil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))

			fail = failure(job.Jid, "uh no again", "YetAnotherError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 1, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))
			assert.EqualValues(t, 2, store.TotalProcessed(bg))
			assert.EqualValues(t, 2, store.TotalFailures(bg))
		})

		t.Run("FailOneShotJob", func(t *testing.T) {
			store.Flush(bg)
			m := newManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			retries := 0
			job.Retry = &retries

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.NotNil(t, m.workingMap[job.Jid])
			assert.Nil(t, m.workingMap[job.Jid].Job.Failure)
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))

			fail := failure(job.Jid, "uh no", "SomeError", nil)
			err = m.Fail(bg, fail)

			assert.NoError(t, err)
			assert.Nil(t, m.workingMap[job.Jid])
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 1, store.TotalFailures(bg))
		})

		t.Run("FailWithInvalidFailPayload", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			err := m.Fail(bg, nil)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing failure info")

			err = m.Fail(bg, &FailPayload{})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing JID")

			err = m.Fail(bg, &FailPayload{Jid: "1238123123"})
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
