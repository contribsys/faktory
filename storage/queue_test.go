package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicQueueOps(t *testing.T) {
	t.Parallel()

	os.RemoveAll("../tmp/queues.db")
	store, err := Open("rocksdb", "queues.db")
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

	data, err = q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
	assert.Equal(t, int64(0), q.Size())

}

func TestQueueKeys(t *testing.T) {
	t.Parallel()

	q := &RocksQueue{
		Name: "foo",
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
