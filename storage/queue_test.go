package storage

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicQueueOps(t *testing.T) {
	t.Parallel()
	defer os.RemoveAll("/tmp/queues.db")

	store, err := Open("rocksdb", "/tmp/queues.db")
	assert.NoError(t, err)
	defer store.Close()
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	assert.Equal(t, int64(0), q.Size())

	data, err := q.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)

	err = q.Push([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, int64(1), q.Size())

	err = q.Push([]byte("world"))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), q.Size())

	values := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	q.Each(func(idx int, key, value []byte) error {
		assert.Equal(t, values[idx], value)
		return nil
	})

	data, err = q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
	assert.Equal(t, int64(1), q.Size())

	cnt, err := q.Clear()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), cnt)
	assert.Equal(t, int64(0), q.Size())
}

func TestDecentQueueUsage(t *testing.T) {
	defer os.RemoveAll("/tmp/qbench.db")
	store, err := Open("rocksdb", "/tmp/qbench.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	assert.Equal(t, int64(0), q.Size())
	err = q.Push([]byte("first"))
	n := 50000
	// Push N jobs to queue
	// Get Size() each time
	for i := 0; i < n; i++ {
		_, data := fakeJob()
		err = q.Push(data)
		assert.NoError(t, err)
		assert.Equal(t, int64(i+2), q.Size())
	}
	err = q.Push([]byte("last"))
	assert.Equal(t, int64(n+2), q.Size())
	// Close DB, reopen
	store.Close()

	store, err = Open("rocksdb", "/tmp/qbench.db")
	assert.NoError(t, err)
	q, err = store.GetQueue("default")
	assert.NoError(t, err)

	// Pop N jobs from queue
	// Get Size() each time
	assert.Equal(t, int64(n+2), q.Size())
	data, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("first"), data)
	for i := 0; i < n; i++ {
		_, err := q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, int64(n-i), q.Size())
	}
	data, err = q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("last"), data)
	assert.Equal(t, int64(0), q.Size())

	data, err = q.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func TestThreadedQueueUsage(t *testing.T) {
	t.Parallel()
	defer os.RemoveAll("/tmp/qthreaded.db")
	store, err := Open("rocksdb", "/tmp/qthreaded.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	tcnt := 5
	n := 10000

	var wg sync.WaitGroup
	for i := 0; i < tcnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pushAndPop(t, n, q)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(0), counter)
	assert.Equal(t, int64(0), q.Size())

	q.Each(func(idx int, k, v []byte) error {
		atomic.AddInt64(&counter, 1)
		//log.Println(string(k), string(v))
		return nil
	})
	assert.Equal(t, int64(0), counter)
	store.Close()
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

func TestQueueKeys(t *testing.T) {
	t.Parallel()

	q := &rocksQueue{
		name: "foo",
		high: 1293712938,
	}
	x := q.nextkey()
	y := q.nextkey()
	z := q.nextkey()
	assert.Equal(t, x[0:3], []byte("foo"))
	assert.Equal(t, x[3], byte(255))
	assert.Equal(t, int64(1293712938), toInt64(x[4:12]))
	assert.Equal(t, int64(1293712939), toInt64(y[4:12]))
	assert.Equal(t, int64(1293712940), toInt64(z[4:12]))

	x = q.nextkey()
	assert.Equal(t, x[0:3], []byte("foo"))
	assert.Equal(t, int64(1293712941), toInt64(x[4:12]))
}

func TestClearAndPush(t *testing.T) {
	defer os.RemoveAll("/tmp/qpush.db")
	store, err := Open("rocksdb", "/tmp/qpush.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("lksjadfl")
	assert.NoError(t, err)

	_, err = q.Clear()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), q.Size())
	q.Push([]byte("123o8123"))
	q.Push([]byte("123o8123"))
	assert.Equal(t, int64(2), q.Size())
	_, err = q.Clear()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), q.Size())
	q.Push([]byte("123o8123"))
	assert.Equal(t, int64(1), q.Size())
}

func BenchmarkQueuePerformance(b *testing.B) {
	defer os.RemoveAll("/tmp/qblah.db")
	store, err := Open("rocksdb", "/tmp/qblah.db")
	assert.NoError(b, err)
	assert.NotNil(b, store)
	defer store.Close()
	q, err := store.GetQueue("default")
	assert.NoError(b, err)

	_, data := fakeJob()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 2 {
		case 0:
			q.Push(data)
		case 1:
			q.Pop()
		}
	}
}

func TestReopening(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/reopening.db")
	store, err := Open("rocksdb", "/tmp/reopening.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)

	c, err := store.GetQueue("critical")
	assert.NoError(t, err)
	d, err := store.GetQueue("default")
	assert.NoError(t, err)
	b, err := store.GetQueue("bulk")
	assert.NoError(t, err)
	e, err := store.GetQueue("emergency")
	assert.NoError(t, err)
	a, err := store.GetQueue("another")
	assert.NoError(t, err)

	err = c.Push([]byte("critical"))
	assert.NoError(t, err)
	err = d.Push([]byte("default"))
	assert.NoError(t, err)
	err = d.Push([]byte("default"))
	assert.NoError(t, err)
	err = b.Push([]byte("bulk"))
	assert.NoError(t, err)
	err = b.Push([]byte("bulk"))
	assert.NoError(t, err)
	err = b.Push([]byte("bulk"))
	assert.NoError(t, err)

	assert.Equal(t, int64(3), b.Size())
	assert.Equal(t, int64(2), d.Size())
	assert.Equal(t, int64(1), c.Size())
	assert.Equal(t, int64(0), e.Size())
	assert.Equal(t, int64(0), a.Size())

	store.Close()

	store, err = Open("rocksdb", "/tmp/reopening.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)

	c, err = store.GetQueue("critical")
	assert.NoError(t, err)
	d, err = store.GetQueue("default")
	assert.NoError(t, err)
	b, err = store.GetQueue("bulk")
	assert.NoError(t, err)
	e, err = store.GetQueue("emergency")
	assert.NoError(t, err)
	a, err = store.GetQueue("another")
	assert.NoError(t, err)

	assert.Equal(t, int64(3), b.Size())
	assert.Equal(t, int64(2), d.Size())
	assert.Equal(t, int64(1), c.Size())
	assert.Equal(t, int64(0), e.Size())
	assert.Equal(t, int64(0), a.Size())

	err = b.Push([]byte("bulk"))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), b.Size())

	var keys [2][]byte
	b.Each(func(idx int, k, v []byte) error {
		if keys[0] == nil {
			cpy := make([]byte, len(k))
			copy(cpy, k)
			keys[0] = cpy
		}
		return nil
	})
	keys[1] = []byte("somefakekey")
	err = b.Delete(keys[0:2])
	assert.NoError(t, err)
	assert.Equal(t, int64(3), b.Size())

	data, err := b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	data, err = b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	data, err = b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, int64(0), b.Size())

	data, err = b.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.Equal(t, int64(0), b.Size())

	store.Close()
}

func TestBlockingPop(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/blocking.db")
	store, err := Open("rocksdb", "/tmp/blocking.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer store.Close()
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	data, err := q.Pop()
	assert.Nil(t, data)
	assert.Nil(t, err)

	// verify we block for 50ms, fruitlessly waiting for a job
	c, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	a := time.Now()
	data, err = q.BPop(c)
	assert.Nil(t, data)
	assert.Nil(t, err)
	b := time.Now()
	assert.True(t, (b.Sub(a) > 5*time.Millisecond))

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		q.Push([]byte("somedata"))
		time.Sleep(2 * time.Millisecond)
		q.Push([]byte("somedata"))
		time.Sleep(2 * time.Millisecond)
		q.Push([]byte("somedata"))
		time.Sleep(50 * time.Millisecond)
		q.Push([]byte("somedata"))
	}()

	var count int
	var timedout int

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()

			c, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			data, err := q.BPop(c)
			assert.NoError(t, err)
			if data != nil {
				count += 1
			}
			if data == nil && err == nil {
				timedout += 1
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 3, count)
	assert.Equal(t, 1, timedout)
	assert.Equal(t, int64(1), q.Size())
	assert.Equal(t, 0, q.(*rocksQueue).waiters.Len())
}
