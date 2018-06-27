package rocksdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/contribsys/faktory/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicQueueOps(t *testing.T) {
	t.Parallel()
	defer os.RemoveAll("/tmp/queues.db")

	store, err := OpenRocks("/tmp/queues.db")
	assert.NoError(t, err)
	defer store.Close()
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	assert.EqualValues(t, 0, q.Size())

	data, err := q.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)

	err = q.Push(5, []byte("hello"))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, q.Size())

	err = q.Push(5, []byte("world"))
	assert.NoError(t, err)
	assert.EqualValues(t, 2, q.Size())

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
	assert.EqualValues(t, 1, q.Size())

	cnt, err := q.Clear()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, cnt)
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
}

func TestQueuePrioritization(t *testing.T) {
	os.RemoveAll("/tmp/qpriority.db")
	defer os.RemoveAll("/tmp/qpriority.db")
	store, err := OpenRocks("/tmp/qpriority.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	assert.EqualValues(t, 0, q.Size())

	n := 100
	// Push N jobs to queue with low priority
	// Get Size() each time
	for i := 0; i < n; i++ {
		err = q.Push(1, []byte("1"))
		assert.NoError(t, err)
		assert.EqualValues(t, i+1, q.Size())
	}

	// Push N jobs to queue with high priority
	// Get Size() each time
	for i := 0; i < n; i++ {
		err = q.Push(3, []byte("3"))
		assert.NoError(t, err)
		assert.EqualValues(t, i+1+n, q.Size())
	}

	// Push N jobs to queue with medium priority
	// Get Size() each time
	for i := 0; i < n; i++ {
		err = q.Push(2, []byte("2"))
		assert.NoError(t, err)
		assert.EqualValues(t, i+1+2*n, q.Size())
	}

	if !assert.EqualValues(t, 3*n, q.Size()) {
		return
	}

	for i := 0; i < n; i++ {
		data, err := q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, []byte("3"), data)
		assert.EqualValues(t, 3*n-(i+1), q.Size())
	}

	for i := 0; i < n; i++ {
		data, err := q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, []byte("2"), data)
		assert.EqualValues(t, 2*n-(i+1), q.Size())
	}

	for i := 0; i < n; i++ {
		data, err := q.Pop()
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), data)
		assert.EqualValues(t, n-(i+1), q.Size())
	}

	// paging starting with empty queue

	err = q.Push(1, []byte("a"))
	assert.NoError(t, err)
	err = q.Push(2, []byte("b"))
	assert.NoError(t, err)
	err = q.Push(3, []byte("c"))
	assert.NoError(t, err)

	// make sure we're paging with priority in mind
	expectations := []struct {
		value    []byte
		index    int
		sequence uint64
		priority uint8
	}{
		{[]byte("c"), 0, 1, 3},
		{[]byte("b"), 1, 1, 2},
		{[]byte("a"), 2, 1, 1},
	}
	count := 0
	err = q.Page(0, 3, func(index int, k, v []byte) error {
		assert.Equal(t, expectations[count].index, index)
		_, priority, seq := decodeKey(q.Name(), k)
		assert.Equal(t, expectations[count].priority, priority)
		assert.Equal(t, expectations[count].sequence, seq)
		assert.Equal(t, expectations[count].value, v)
		count++
		return nil
	})
	assert.NoError(t, err)
}

func TestDecentQueueUsage(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/qbench.db")
	store, err := OpenRocks("/tmp/qbench.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	assert.EqualValues(t, 0, q.Size())
	err = q.Push(5, []byte("first"))
	assert.NoError(t, err)
	n := 5000
	// Push N jobs to queue
	// Get Size() each time
	for i := 0; i < n; i++ {
		_, data := fakeJob()
		err = q.Push(5, data)
		assert.NoError(t, err)
		assert.EqualValues(t, i+2, q.Size())
	}

	err = q.Push(5, []byte("last"))
	assert.NoError(t, err)
	assert.EqualValues(t, n+2, q.Size())

	// Close DB, reopen
	store.Close()

	store, err = OpenRocks("/tmp/qbench.db")
	assert.NoError(t, err)

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
}

func TestThreadedQueueUsage(t *testing.T) {
	t.Parallel()
	defer os.RemoveAll("/tmp/qthreaded.db")
	store, err := OpenRocks("/tmp/qthreaded.db")
	assert.NoError(t, err)
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

	q.Each(func(idx int, k, v []byte) error {
		atomic.AddInt64(&counter, 1)
		//log.Println(string(k), string(v))
		return nil
	})
	assert.EqualValues(t, 0, counter)
	store.Close()
}

var (
	counter int64
)

func pushAndPop(t *testing.T, n int, q types.Queue) {
	for i := 0; i < n; i++ {
		_, data := fakeJob()
		err := q.Push(5, data)
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
		pointers: map[uint8]*queuePointer{
			5: &queuePointer{
				priority: 5,
				high:     1293712938,
			},
		},
	}
	x := q.nextkey(5)
	y := q.nextkey(5)
	z := q.nextkey(5)
	_, _, seqX := decodeKey("foo", x)
	_, _, seqY := decodeKey("foo", y)
	_, _, seqZ := decodeKey("foo", z)
	assert.EqualValues(t, 1293712939, seqX)
	assert.EqualValues(t, 1293712940, seqY)
	assert.EqualValues(t, 1293712941, seqZ)

	x = q.nextkey(5)
	_, _, seqX = decodeKey("foo", x)
	assert.EqualValues(t, 1293712942, seqX)
}

func TestClearAndPush(t *testing.T) {
	defer os.RemoveAll("/tmp/qpush.db")
	store, err := OpenRocks("/tmp/qpush.db")
	assert.NoError(t, err)
	q, err := store.GetQueue("lksjadfl")
	assert.NoError(t, err)

	_, err = q.Clear()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, q.Size())
	q.Push(5, []byte("123o8123"))
	q.Push(5, []byte("123o8123"))
	assert.EqualValues(t, 2, q.Size())
	_, err = q.Clear()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, q.Size())
	q.Push(5, []byte("123o8123"))
	assert.EqualValues(t, 1, q.Size())
}

func BenchmarkQueuePerformance(b *testing.B) {
	defer os.RemoveAll("/tmp/qblah.db")
	store, err := OpenRocks("/tmp/qblah.db")
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
			q.Push(5, data)
		case 1:
			q.Pop()
		}
	}
}

func TestReopening(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/reopening.db")
	store, err := OpenRocks("/tmp/reopening.db")
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

	err = c.Push(5, []byte("critical"))
	assert.NoError(t, err)
	err = d.Push(5, []byte("default"))
	assert.NoError(t, err)
	err = d.Push(5, []byte("default"))
	assert.NoError(t, err)
	err = b.Push(5, []byte("bulk"))
	assert.NoError(t, err)
	err = b.Push(5, []byte("bulk"))
	assert.NoError(t, err)
	err = b.Push(5, []byte("bulk"))
	assert.NoError(t, err)

	assert.EqualValues(t, 3, b.Size())
	assert.EqualValues(t, 2, d.Size())
	assert.EqualValues(t, 1, c.Size())
	assert.EqualValues(t, 0, e.Size())
	assert.EqualValues(t, 0, a.Size())

	store.Close()

	store, err = OpenRocks("/tmp/reopening.db")
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

	store.EachQueue(func(q types.Queue) {
		fmt.Println(q.Name(), q.Size())
	})

	assert.EqualValues(t, 3, b.Size())
	assert.EqualValues(t, 2, d.Size())
	assert.EqualValues(t, 1, c.Size())
	assert.EqualValues(t, 0, e.Size())
	assert.EqualValues(t, 0, a.Size())

	err = b.Push(5, []byte("bulk"))
	assert.NoError(t, err)
	assert.EqualValues(t, 4, b.Size())

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
	assert.EqualValues(t, 3, b.Size())

	data, err := b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	data, err = b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	data, err = b.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.EqualValues(t, 0, b.Size())

	data, err = b.Pop()
	assert.NoError(t, err)
	assert.Nil(t, data)
	assert.EqualValues(t, 0, b.Size())

	store.Close()
}

func TestBlockingPop(t *testing.T) {
	defer os.RemoveAll("/tmp/blocking.db")
	store, err := OpenRocks("/tmp/blocking.db")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer store.Close()
	q, err := store.GetQueue("default")
	assert.NoError(t, err)

	data, err := q.Pop()
	assert.Nil(t, data)
	assert.Nil(t, err)

	a := time.Now()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	data, err = q.BPop(c)
	assert.Nil(t, data)
	assert.Nil(t, err)
	b := time.Now()
	diff := b.Sub(a)
	assert.True(t, (diff > 5*time.Millisecond), fmt.Sprintf("%v", diff))

	var wg sync.WaitGroup
	wg.Add(1)

	// verify we block for 50ms, fruitlessly waiting for a job
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		q.Push(5, []byte("somedata"))
		time.Sleep(5 * time.Millisecond)
		q.Push(5, []byte("somedata"))
		time.Sleep(5 * time.Millisecond)
		q.Push(5, []byte("somedata"))
		time.Sleep(200 * time.Millisecond)
		q.Push(5, []byte("somedata"))
	}()

	var count int
	var timedout int
	var waitMutex sync.Mutex

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			c, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			data, err := q.BPop(c)
			assert.NoError(t, err)
			waitMutex.Lock()
			defer waitMutex.Unlock()
			if data != nil {
				count++
			}
			if data == nil && err == nil {
				timedout++
			}
		}()
	}

	wg.Wait()

	assert.EqualValues(t, 3, count)
	assert.EqualValues(t, 1, timedout)
	assert.EqualValues(t, 1, q.Size())
	assert.EqualValues(t, 0, q.(*rocksQueue).waiters.Len())

	q.Clear()

	// verify we have four waiters and they are released
	// immediately upon q.Close()
	wg = sync.WaitGroup{}
	rq := q.(*rocksQueue)
	var nothing int64

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			data, err := q.BPop(c)
			if data == nil && err == nil {
				atomic.AddInt64(&nothing, 1)
			}
			assert.NoError(t, err)
		}()
	}

	for i := 0; i < 100; i++ {
		time.Sleep(1 * time.Millisecond)
		rq.waitmu.RLock()
		if rq.waiters.Len() == 4 {
			rq.waitmu.RUnlock()
			break
		}
		rq.waitmu.RUnlock()
	}

	rq.Close()
	wg.Wait()
	assert.EqualValues(t, 4, nothing)
}
