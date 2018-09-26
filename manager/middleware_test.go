package manager

import (
	"errors"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestMiddlewareUsage(t *testing.T) {
	myMiddleware := make(MiddlewareChain, 0)

	counter := 0

	fn := func(next func() error, job *client.Job) error {
		counter += 1
		return next()
	}
	fn2 := func(next func() error, job *client.Job) error {
		counter += 1
		return next()
	}
	assert.Equal(t, 0, len(myMiddleware))
	myMiddleware = append(myMiddleware, fn)
	assert.Equal(t, 1, len(myMiddleware))
	myMiddleware = append(myMiddleware, fn2)
	assert.Equal(t, 2, len(myMiddleware))

	job := client.NewJob("Something", 1, 2)

	err := callMiddleware(myMiddleware, job, func() error {
		counter += 1
		return nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 3, counter)
}

func TestLiveMiddleware(t *testing.T) {
	denied := errors.New("push denied")

	withRedis(t, "middleware", func(t *testing.T, store storage.Store) {

		t.Run("Push", func(t *testing.T) {
			store.Flush()
			m := NewManager(store)
			m.AddMiddleware("push", func(next func() error, job *client.Job) error {
				if job.Type == "Nope" {
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
	})
}
