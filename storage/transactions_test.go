package storage

import (
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestEnqueueAll(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/xa.db")
	db, err := Open("rocksdb", "/tmp/xa.db")
	assert.NoError(t, err)
	defer db.Close()

	jid1, j1 := fakeJob()
	jid2, j2 := fakeJob()
	past := time.Now()

	r := db.Retries()
	err = r.AddElement(util.Thens(past), jid1, j1)
	assert.NoError(t, err)
	err = r.AddElement(util.Thens(past), jid2, j2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), r.Size())

	err = db.EnqueueAll(r)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), r.Size())

	q, err := db.GetQueue("default")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), q.Size())

	job, err := q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, job)
	job, err = q.Pop()
	assert.NoError(t, err)
	assert.NotNil(t, job)
	job, err = q.Pop()
	assert.NoError(t, err)
	assert.Nil(t, job)
}
