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

func TestManagerBasics(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"b", "c"}, filter([]string{"a"}, []string{"a", "b", "c"}))
	assert.Equal(t, []string{"a"}, filter([]string{"c", "b"}, []string{"a", "b", "c"}))
}

func TestManager(t *testing.T) {
	withRedis(t, "manager", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		t.Run("Push", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.NotEmpty(t, job.EnqueuedAt)
		})

		t.Run("PushJobWithInvalidId", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			_, _ = q.Clear(bg)
			assert.EqualValues(t, 0, q.Size(bg))

			jids := []string{"", "id", "shortid"}
			for _, jid := range jids {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.Queue = "default"
				job.Jid = jid
				assert.EqualValues(t, 0, q.Size(bg))
				assert.Empty(t, job.EnqueuedAt)

				err = m.Push(bg, job)

				assert.Error(t, err)
				assert.EqualValues(t, 0, q.Size(bg))
				assert.Empty(t, job.EnqueuedAt)
			}
		})

		t.Run("PushJobWithInvalidType", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushJobWithoutArgs", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("NoArgs")
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJob", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			future := time.Now().Add(time.Duration(5) * time.Minute)
			job.At = util.Thens(future)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Scheduled().Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Scheduled().Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJobWithPastTime", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			oneMinuteAgo := time.Now().Add(-time.Duration(1) * time.Second)
			job.At = util.Thens(oneMinuteAgo)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.NotEmpty(t, job.EnqueuedAt)
		})

		t.Run("PushScheduledJobWithInvalidTime", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ScheduledJob", 1, 2, 3)
			job.At = "invalid time"
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			_, _ = q.Clear(bg)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(bg, job)

			assert.Error(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.Empty(t, job.EnqueuedAt)
		})

		t.Run("Fetch", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))

			queues := []string{"default"}
			fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q.Size(bg))
		})

		t.Run("EmptyFetch", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			queues := []string{}
			job, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.Nil(t, job)
			assert.Error(t, err)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			queues = []string{"default"}
			fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
			assert.NoError(t, err)
			assert.Nil(t, fetchedJob)
		})

		t.Run("FetchWithPause", func(t *testing.T) {
			store.Flush(bg)

			dq, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.NoError(t, dq.Pause(bg))

			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q1, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q1.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q1.Size(bg))

			email := client.NewJob("SendEmail", 1, 2, 3)
			email.Queue = "email"
			q2, err := store.GetQueue(bg, email.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q2.Size(bg))

			err = m.Push(bg, email)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q2.Size(bg))

			queues := []string{"default", "email"}

			fetchedJob, err := m.Fetch(bg, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, email.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 1, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))

			assert.NoError(t, m.ResumeQueue(bg, "default"))

			fetchedJob, err = m.Fetch(bg, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))

			pq, err := store.PausedQueues(bg)
			assert.NoError(t, err)
			assert.Equal(t, []string{}, pq)

			assert.NoError(t, m.PauseQueue(bg, "default"))

			pq, err = store.PausedQueues(bg)
			assert.NoError(t, err)
			assert.Equal(t, []string{"default"}, pq)
		})

		t.Run("FetchFromMultipleQueues", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			job := client.NewJob("ManagerPush", 1, 2, 3)
			q1, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q1.Size(bg))

			err = m.Push(bg, job)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q1.Size(bg))

			email := client.NewJob("SendEmail", 1, 2, 3)
			email.Queue = "email"
			q2, err := store.GetQueue(bg, email.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q2.Size(bg))

			err = m.Push(bg, email)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, q2.Size(bg))

			queues := []string{"default", "email"}

			fetchedJob, err := m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, job.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 1, q2.Size(bg))

			fetchedJob, err = m.Fetch(context.Background(), "workerId", queues...)
			assert.NoError(t, err)
			assert.NotNil(t, fetchedJob)
			assert.EqualValues(t, email.Jid, fetchedJob.Jid)
			assert.EqualValues(t, 0, q1.Size(bg))
			assert.EqualValues(t, 0, q2.Size(bg))
		})

		t.Run("FetchAwaitsForNewJob", func(t *testing.T) {
			store.Flush(bg)
			m := NewManager(store)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			go func() {
				time.Sleep(time.Duration(1) * time.Second)

				t.Log("Pushing job")
				job := client.NewJob("ManagerPush", 1, 2, 3)
				err = m.Push(bg, job)
				assert.NoError(t, err)
			}()

			queues := []string{"default"}
			fetchedJob, err := m.Fetch(ctx, "workerId", queues...)
			assert.NoError(t, err)
			assert.NotEmpty(t, fetchedJob)
		})
	})
}

func withRedis(t *testing.T, name string, fn func(*testing.T, storage.Store)) {
	t.Parallel()

	dir := fmt.Sprintf("/tmp/faktory-test-%s", name)
	defer os.RemoveAll(dir)

	sock := fmt.Sprintf("%s/redis.sock", dir)
	stopper, err := storage.Boot(dir, sock)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	store, err := storage.Open(sock, 10)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	fn(t, store)

}
