package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestMiddlewareUsage(t *testing.T) {
	myMiddleware := make(MiddlewareChain, 0)

	counter := 0

	fn := func(next func() error, ctx Context) error {
		counter += 1
		return next()
	}
	fn2 := func(next func() error, ctx Context) error {
		counter += 1
		return next()
	}
	assert.Equal(t, 0, len(myMiddleware))
	myMiddleware = append(myMiddleware, fn)
	assert.Equal(t, 1, len(myMiddleware))
	myMiddleware = append(myMiddleware, fn2)
	assert.Equal(t, 2, len(myMiddleware))

	job := client.NewJob("Something", 1, 2)

	err := callMiddleware(myMiddleware, Ctx{context.Background(), job, nil}, func() error {
		counter += 1
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 3, counter)
}

func TestLiveMiddleware(t *testing.T) {

	withRedis(t, "middleware", func(t *testing.T, store storage.Store) {

		t.Run("Push", func(t *testing.T) {
			denied := errors.New("push denied")
			store.Flush()
			m := NewManager(store)
			m.AddMiddleware("push", func(next func() error, ctx Context) error {
				if ctx.Job().Type == "Nope" {
					return denied
				}
				return next()
			})

			job := client.NewJob("Yep", 1, 2, 3)
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())
			assert.Empty(t, job.EnqueuedAt)

			err = m.Push(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())
			assert.NotEmpty(t, job.EnqueuedAt)

			job = client.NewJob("Nope", 1, 2, 3)
			err = m.Push(job)
			assert.Equal(t, err, denied)
			assert.EqualValues(t, 1, q.Size())
		})

		t.Run("Fetch", func(t *testing.T) {
			denied := errors.New("fetch denied")

			store.Flush()
			m := NewManager(store)
			m.AddMiddleware("fetch", func(next func() error, ctx Context) error {
				kv := ctx.Manager().KV()

				err := kv.Set("foo", []byte("bar"))
				assert.NoError(t, err)
				val, err := kv.Get("foo")
				assert.NoError(t, err)
				assert.Equal(t, "bar", string(val))

				if ctx.Job().Type == "Nope" {
					return denied
				}
				return next()
			})

			job := client.NewJob("Yep", 1, 2, 3)
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())

			err = m.Push(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())

			job = client.NewJob("Nope", 1, 2, 3)
			err = m.Push(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size())

			j1, err := m.Fetch(context.Background(), "12345", "default")
			assert.NoError(t, err)
			assert.Equal(t, "Yep", j1.Type)
			assert.EqualValues(t, 1, q.Size())

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			j2, err := m.Fetch(ctx, "12345", "default")
			assert.Equal(t, err, denied)
			assert.Nil(t, j2)
			assert.EqualValues(t, 0, q.Size())
		})

		t.Run("Ack", func(t *testing.T) {
			store.Flush()
			m := NewManager(store)
			m.AddMiddleware("push", func(next func() error, ctx Context) error {
				_, err := m.Redis().Set(ctx.Job().Jid, []byte("bar"), 1*time.Second).Result()
				assert.NoError(t, err)
				return next()
			})
			m.AddMiddleware("ack", func(next func() error, ctx Context) error {
				val, err := m.Redis().Del(ctx.Job().Jid).Result()
				assert.NoError(t, err)
				assert.EqualValues(t, 1, val)
				return next()
			})

			job := client.NewJob("Yep", 1, 2, 3)
			q, err := store.GetQueue(job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size())

			jid := job.Jid

			err = m.Push(job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())

			val, err := m.Redis().Get(jid).Result()
			assert.NoError(t, err)
			assert.Equal(t, "bar", string(val))

			j1, err := m.Fetch(context.Background(), "12345", "default")
			assert.NoError(t, err)
			assert.Equal(t, "Yep", j1.Type)
			assert.EqualValues(t, 0, q.Size())

			job, err = m.Acknowledge(j1.Jid)
			assert.NoError(t, err)

			boolint, err := m.Redis().Exists(job.Jid).Result()
			assert.NoError(t, err)
			assert.EqualValues(t, 0, boolint)
		})

	})
}
