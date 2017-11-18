package manager

import (
	"context"
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

func TestManagerFetch(t *testing.T) {
	t.Run("Fetch", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ManagerPush", 1, 2, 3)
		q, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())

		err = m.Push(job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, q.Size())

		queues := []string{"default"}
		fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
		assert.NoError(t, err)
		assert.EqualValues(t, job.Jid, fetchedJob.Jid)
		assert.EqualValues(t, 0, q.Size())
	})

	t.Run("FetchFromEmptyQueue", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		q, err := store.GetQueue("default")
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		queues := []string{"default"}
		fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
		assert.NoError(t, err)
		assert.Empty(t, fetchedJob)
	})

	t.Run("FetchFromMultipleQueues", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		job := client.NewJob("ManagerPush", 1, 2, 3)
		q1, err := store.GetQueue(job.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q1.Size())

		err = m.Push(job)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, q1.Size())

		email := client.NewJob("SendEmail", 1, 2, 3)
		email.Queue = "email"
		q2, err := store.GetQueue(email.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q2.Size())

		err = m.Push(email)

		assert.NoError(t, err)
		assert.EqualValues(t, 1, q2.Size())

		queues := []string{"default", "email"}

		fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
		assert.NoError(t, err)
		assert.EqualValues(t, job.Jid, fetchedJob.Jid)
		assert.EqualValues(t, 0, q1.Size())
		assert.EqualValues(t, 1, q2.Size())

		fetchedJob, err = m.Fetch(context.Background(), "workerId", queues...)
		assert.NoError(t, err)
		assert.EqualValues(t, email.Jid, fetchedJob.Jid)
		assert.EqualValues(t, 0, q1.Size())
		assert.EqualValues(t, 0, q2.Size())
	})

	t.Run("FetchAwaitsForNewJob", func(t *testing.T) {
		t.Parallel()
		store, teardown := setupTest(t)
		defer teardown(t)

		m := NewManager(store)

		q, err := store.GetQueue("default")
		assert.NoError(t, err)
		assert.EqualValues(t, 0, q.Size())

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		go func() {
			time.Sleep(time.Duration(1) * time.Second)

			t.Log("Pushing job")
			job := client.NewJob("ManagerPush", 1, 2, 3)
			err = m.Push(job)
			assert.NoError(t, err)
		}()

		queues := []string{"default"}
		fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
		assert.NoError(t, err)
		assert.NotEmpty(t, fetchedJob)
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
