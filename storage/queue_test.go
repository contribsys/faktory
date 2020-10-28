package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicQueueOps(t *testing.T) {
	withRedis(t, "queue", func(t *testing.T, store Store) {

		t.Run("Push", func(t *testing.T) {
			store.Flush()
			q, err := store.GetQueue("default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size())

			data, err := q.Pop()
			assert.NoError(t, err)
			assert.Nil(t, data)

			err = q.Push([]byte("hello"))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size())

			err = q.Push([]byte("world"))
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size())

			values := [][]byte{
				[]byte("world"),
				[]byte("hello"),
			}
			err = q.Each(func(idx int, value []byte) error {
				assert.Equal(t, values[idx], value)
				return nil
			})
			assert.NoError(t, err)

			data, err = q.Pop()
			assert.NoError(t, err)
			assert.Equal(t, []byte("hello"), data)
			assert.EqualValues(t, 1, q.Size())

			cnt, err := q.Clear()
			assert.NoError(t, err)
			assert.EqualValues(t, 0, cnt)
			assert.EqualValues(t, 0, q.Size())

			// valid names:
			_, err = store.GetQueue("A-Za-z0-9_.-")
			assert.NoError(t, err)
			_, err = store.GetQueue("-")
			assert.NoError(t, err)
			_, err = store.GetQueue("A")
			assert.NoError(t, err)
			_, err = store.GetQueue("a")
			assert.NoError(t, err)

			// invalid names:
			_, err = store.GetQueue("default?page=1")
			assert.Error(t, err)
			_, err = store.GetQueue("user@example.com")
			assert.Error(t, err)
			_, err = store.GetQueue("c&c")
			assert.Error(t, err)
			_, err = store.GetQueue("priority|high")
			assert.Error(t, err)
			_, err = store.GetQueue("")
			assert.Error(t, err)
		})

		t.Run("heavy", func(t *testing.T) {
			store.Flush()
			q, err := store.GetQueue("default")
			assert.NoError(t, err)

			assert.EqualValues(t, 0, q.Size())
			err = q.Push([]byte("first"))
			assert.NoError(t, err)
			n := 5000
			// Push N jobs to queue
			// Get Size() each time
			for i := 0; i < n; i++ {
				_, data := fakeJob()
				err = q.Push(data)
				assert.NoError(t, err)
				assert.EqualValues(t, i+2, q.Size())
			}

			err = q.Push([]byte("last"))
			assert.NoError(t, err)
			assert.EqualValues(t, n+2, q.Size())

			q, err = store.GetQueue("default")
			assert.NoError(t, err)

			// Pop N jobs from queue
			// Get Size() each time
			assert.EqualValues(t, n+2, q.Size())
			data, err := q.Pop()
			assert.NoError(t, err)
			assert.Equal(t, []byte("first"), data)
			for i := 0; i < n; i++ {
				_, err := q.Pop()
				assert.NoError(t, err)
				assert.EqualValues(t, n-i, q.Size())
			}
			data, err = q.Pop()
			assert.NoError(t, err)
			assert.Equal(t, []byte("last"), data)
			assert.EqualValues(t, 0, q.Size())

			data, err = q.Pop()
			assert.NoError(t, err)
			assert.Nil(t, data)
		})

		t.Run("threaded", func(t *testing.T) {
			store.Flush()
			q, err := store.GetQueue("default")
			assert.NoError(t, err)

			tcnt := 5
			n := 1000

			var wg sync.WaitGroup
			for i := 0; i < tcnt; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pushAndPop(t, n, q)
				}()
			}

			wg.Wait()
			assert.EqualValues(t, 0, counter)
			assert.EqualValues(t, 0, q.Size())

			err = q.Each(func(idx int, v []byte) error {
				atomic.AddInt64(&counter, 1)
				//log.Println(string(k), string(v))
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 0, counter)
		})
	})
}

var (
	counter int64
)

func pushAndPop(t *testing.T, n int, q Queue) {
	for i := 0; i < n; i++ {
		_, data := fakeJob()
		err := q.Push(data)
		assert.NoError(t, err)
		atomic.AddInt64(&counter, 1)
	}

	for i := 0; i < n; i++ {
		value, err := q.Pop()
		assert.NoError(t, err)
		assert.NotNil(t, value)
		atomic.AddInt64(&counter, -1)
	}
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	nows := util.Nows()
	return jid, []byte(fmt.Sprintf(`{"jid":"%s","created_at":"%s","queue":"default","args":[1,2,3],"class":"SomeWorker"}`, jid, nows))
}
