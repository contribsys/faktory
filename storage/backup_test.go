package storage

import (
	"os"
	"testing"

	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBackupAndRestore(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/backup.db")
	// open db
	db, err := Open("rocksdb", "/tmp/backup.db")
	assert.NoError(t, err)

	// put elements
	q, err := db.GetQueue("default")
	assert.NoError(t, err)
	q.Push(5, []byte("f"))
	q.Push(5, []byte("fo"))
	assert.Equal(t, uint64(2), q.Size())

	rs := db.Retries()
	rs.AddElement(util.Nows(), "foobar", []byte("thepayload"))
	assert.Equal(t, int64(1), rs.Size())

	count := 0
	db.EachBackup(func(element BackupInfo) {
		count++
	})
	assert.Equal(t, 0, count)

	// take backup
	err = db.Backup()
	assert.NoError(t, err)
	count = 0
	db.EachBackup(func(element BackupInfo) {
		count++
	})
	assert.Equal(t, 1, count)

	// put more elements
	q.Push(5, []byte("foo"))
	assert.Equal(t, uint64(3), q.Size())

	// restore from backup
	err = db.RestoreFromLatest()
	assert.NoError(t, err)

	db, err = Open("rocksdb", "/tmp/backup.db")
	assert.NoError(t, err)

	// verify elements
	q, err = db.GetQueue("default")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), q.Size())

	elm, err := q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("f"), elm)

	assert.Equal(t, int64(1), db.Retries().Size())

	// flush the entire db
	// backup
	// add item to queue
	// restore
	// verify the queue is empty

	err = db.Flush()
	assert.NoError(t, err)

	q, err = db.GetQueue("default")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), q.Size())

	err = db.Backup()
	assert.NoError(t, err)

	q, err = db.GetQueue("default")
	assert.NoError(t, err)

	err = q.Push(5, []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), q.Size())

	err = db.RestoreFromLatest()
	assert.NoError(t, err)

	db, err = Open("rocksdb", "/tmp/backup.db")
	assert.NoError(t, err)

	q, err = db.GetQueue("default")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), q.Size())
}
