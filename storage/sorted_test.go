package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/mperham/faktory/util"
	"github.com/stretchr/testify/assert"
)

func init() {
	DefaultPath = "../tmp"
	os.Mkdir("../tmp", os.FileMode(os.ModeDir|0755))
}

func TestBasicSortedSet(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("../tmp/sorted.db")
	db, err := Open("rocksdb", "sorted.db")
	assert.NoError(t, err)
	defer db.Close()

	jid, j1 := fakeJob()
	past := time.Now()

	r := db.Retries()
	assert.Equal(t, int64(0), r.Size())
	err = r.AddElement(util.Thens(past), fmt.Sprintf("0%s", jid), j1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), r.Size())

	jid, j2 := fakeJob()
	err = r.AddElement(util.Thens(past), fmt.Sprintf("1%s", jid), j2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), r.Size())

	current := time.Now()
	jid, j3 := fakeJob()
	err = r.AddElement(util.Thens(current.Add(10*time.Second)), jid, j3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), r.Size())

	results, err := r.RemoveBefore(util.Thens(current.Add(1 * time.Second)))
	assert.NoError(t, err)
	assert.Equal(t, int64(1), r.Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{j1, j2}
	assert.Equal(t, values, results)
}

func TestRocksSortedSet(b *testing.T) {
	b.Parallel()
	defer os.RemoveAll("../tmp/rocks.db")

	db, err := Open("rocksdb", "rocks.db")
	assert.NoError(b, err)
	defer db.Close()

	count := int64(1000)
	retries := db.Retries()
	start := time.Now()
	for i := int64(0); i < count; i++ {
		jid, job := fakeJob()
		err = retries.AddElement(util.Thens(start.Add(time.Duration(rand.Intn(int(10*count)))*time.Second)), jid, job)
		assert.NoError(b, err)
	}

	amt := int64(0)
	ajid := ""
	atstamp := ""
	err = retries.EachElement(func(tstamp string, jid string, elm []byte) error {
		ajid = jid
		atstamp = tstamp
		assert.Equal(b, 27, len(tstamp))
		assert.Equal(b, 16, len(jid))
		assert.NotNil(b, elm)
		amt += int64(1)
		return nil
	})
	assert.NoError(b, err)
	assert.Equal(b, count, amt)

	assert.Equal(b, int64(0), db.Working().Size())
	err = retries.MoveTo(db.Working(), atstamp, ajid, func(payload []byte) (string, []byte, error) {
		return util.Nows(), payload, nil
	})
	assert.NoError(b, err)
	assert.Equal(b, int64(1), db.Working().Size())
	assert.Equal(b, count-1, retries.Size())
	count -= 1

	err = retries.MoveTo(db.Working(), "1231", ajid, func(payload []byte) (string, []byte, error) {
		return util.Nows(), payload, nil
	})
	assert.Error(b, err)

	remd := int64(0)
	start = time.Now()
	for i := int64(0); i < count; i++ {
		elms, err := retries.RemoveBefore(util.Thens(start.Add(time.Duration(rand.Intn(int(5*count))) * time.Second)))
		assert.NoError(b, err)
		remd += int64(len(elms))
		assert.Equal(b, count-remd, retries.Size())
		//assert.True(b, len(elms) == 0 || len(elms) == 1 || len(elms) == 2)
	}
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	return jid, []byte(`{"jid":"` + jid + `","created_at":1234567890123123123,"queue":"default","args":[1,2,3],"class":"SomeWorker"}`)
}
