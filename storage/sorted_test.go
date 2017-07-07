package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/mperham/worq/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicSortedSet(t *testing.T) {
	t.Parallel()

	os.RemoveAll("sorted.db")
	db, err := Open("rocksdb", "sorted.db")
	assert.NoError(t, err)
	jid, j1 := fakeJob()

	past := time.Now()

	r := db.Retries()
	assert.Equal(t, 0, r.Size())
	err = r.AddElement(util.Thens(past), fmt.Sprintf("0%s", jid), j1)
	assert.NoError(t, err)
	assert.Equal(t, 1, r.Size())

	jid, j2 := fakeJob()
	err = r.AddElement(util.Thens(past), fmt.Sprintf("1%s", jid), j2)
	assert.NoError(t, err)
	assert.Equal(t, 2, r.Size())

	current := time.Now()
	jid, j3 := fakeJob()
	err = r.AddElement(util.Thens(current.Add(10*time.Second)), jid, j3)
	assert.NoError(t, err)
	assert.Equal(t, 3, r.Size())

	results, err := r.RemoveBefore(util.Thens(current.Add(1 * time.Second)))
	assert.NoError(t, err)
	assert.Equal(t, 1, r.Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{j1, j2}
	assert.Equal(t, values, results)
}

func TestBoltSortedSet(b *testing.T) {
	b.Parallel()
	os.RemoveAll("basic.db")
	db, err := Open("rocksdb", "basic.db")
	count := 100
	assert.NoError(b, err)

	retries := db.Retries()
	start := time.Now()
	for i := 0; i < count; i++ {
		jid, job := fakeJob()
		err = retries.AddElement(util.Thens(start.Add(time.Duration(rand.Intn(10*count))*time.Second)), jid, job)
		assert.NoError(b, err)
		assert.Equal(b, i+1, retries.Size())
	}

	amt := 0
	err = retries.EachElement(func(tstamp string, jid string, elm []byte) error {
		assert.Equal(b, 27, len(tstamp))
		assert.Equal(b, 16, len(jid))
		assert.NotNil(b, elm)
		amt += 1
		return nil
	})
	assert.NoError(b, err)
	assert.Equal(b, count, amt)

	remd := 0
	start = time.Now()
	for i := 0; i < count; i++ {
		elms, err := retries.RemoveBefore(util.Thens(start.Add(time.Duration(rand.Intn(5*count)) * time.Second)))
		assert.NoError(b, err)
		remd += len(elms)
		assert.Equal(b, count-remd, retries.Size())
		//assert.True(b, len(elms) == 0 || len(elms) == 1 || len(elms) == 2)
	}
}

func TestRocksSortedSet(b *testing.T) {
	b.Parallel()
	os.RemoveAll("rocks.db")
	db, err := Open("rocksdb", "rocks.db")
	count := 1000
	assert.NoError(b, err)

	retries := db.Retries()
	start := time.Now()
	for i := 0; i < count; i++ {
		jid, job := fakeJob()
		err = retries.AddElement(util.Thens(start.Add(time.Duration(rand.Intn(10*count))*time.Second)), jid, job)
		assert.NoError(b, err)
	}

	amt := 0
	ajid := ""
	atstamp := ""
	err = retries.EachElement(func(tstamp string, jid string, elm []byte) error {
		ajid = jid
		atstamp = tstamp
		assert.Equal(b, 27, len(tstamp))
		assert.Equal(b, 16, len(jid))
		assert.NotNil(b, elm)
		amt += 1
		return nil
	})
	assert.NoError(b, err)
	assert.Equal(b, count, amt)

	assert.Equal(b, 0, db.Working().Size())
	err = retries.MoveTo(db.Working(), atstamp, ajid, func(payload []byte) (string, []byte, error) {
		return util.Nows(), payload, nil
	})
	assert.NoError(b, err)
	assert.Equal(b, 1, db.Working().Size())
	assert.Equal(b, count-1, retries.Size())
	count -= 1

	err = retries.MoveTo(db.Working(), "1231", ajid, func(payload []byte) (string, []byte, error) {
		return util.Nows(), payload, nil
	})
	assert.Error(b, err)

	remd := 0
	start = time.Now()
	for i := 0; i < count; i++ {
		elms, err := retries.RemoveBefore(util.Thens(start.Add(time.Duration(rand.Intn(5*count)) * time.Second)))
		assert.NoError(b, err)
		remd += len(elms)
		assert.Equal(b, count-remd, retries.Size())
		//assert.True(b, len(elms) == 0 || len(elms) == 1 || len(elms) == 2)
	}
	db.Close()
}

func fakeJob() (string, []byte) {
	jid := util.RandomJid()
	return jid, []byte(`{"jid":"` + jid + `","created_at":1234567890123123123,"queue":"default","args":[1,2,3],"class":"SomeWorker"}`)
}
