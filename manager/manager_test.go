package manager

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestManagerPush(t *testing.T) {
	t.Run("Push", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ManagerPush", 1, 2, 3)
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, q.Size())
		assert.NotEmpty(t, job.EnqueuedAt)
	})

	t.Run("PushJobWithInvalidId", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		jids := []string{"", "id", "shortid"}
		for _, jid := range jids {
			job := client.NewJob("InvalidJob", 1, 2, 3)
			job.Jid = jid
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.Empty(t, job.EnqueuedAt)
		}
	})

	t.Run("PushJobWithInvalidType", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("", 1, 2, 3)
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.Error(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)
	})

	t.Run("PushJobWithoutArgs", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("NoArgs")
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.Error(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)
	})

	t.Run("PushJobWithInvalidPriority", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		priorities := []uint8{0, 10, 11, 20, 42}
		for _, p := range priorities {
			job := client.NewJob("SomeJob", 1, 2, 3)
			job.Priority = p
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())
			assert.NotEmpty(t, job.EnqueuedAt)
			assert.EqualValues(t, 5, job.Priority)

			q.Clear()
		}
	})

	t.Run("PushScheduledJob", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ScheduledJob", 1, 2, 3)
		future := time.Now().Add(time.Duration(5) * time.Minute)
		job.At = util.Thens(future)
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.EqualValues(t, 0, store.Scheduled().Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.EqualValues(t, 1, store.Scheduled().Size())
		assert.Empty(t, job.EnqueuedAt)
	})

	t.Run("PushScheduledJobWithPastTime", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ScheduledJob", 1, 2, 3)
		oneMinuteAgo := time.Now().Add(-time.Duration(1) * time.Second)
		job.At = util.Thens(oneMinuteAgo)
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, q.Size())
		assert.NotEmpty(t, job.EnqueuedAt)
	})

	t.Run("PushScheduledJobWithInvalidTime", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ScheduledJob", 1, 2, 3)
		job.At = "invalid time"
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)

		err = m.Push(job)

		assert.Error(t, err)
		assert.EqualValues(t, 0, q.Size())
		assert.Empty(t, job.EnqueuedAt)
	})
}

func setupTest(t *testing.T) (storage.Store, func(t *testing.T)) {
	path := fmt.Sprintf("/tmp/%s.db", t.Name())
	store, err := storage.Open("rocksdb", path)
	if err != nil {
		panic(err)
	}
	return store, func(t *testing.T) {
		os.RemoveAll(path)
	}
}
