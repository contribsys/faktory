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

func TestBasicTimedSet(t *testing.T) {
	t.Parallel()

	db, err := Open("rocksdb", "../test/timed")
	assert.NoError(t, err)
	j1 := []byte(fakeJob())

	past := time.Now()

	r := db.Retries()
	assert.Equal(t, 0, r.Size())
	err = r.AddElement(util.Thens(past), "1239712983", j1)
	assert.NoError(t, err)
	assert.Equal(t, 1, r.Size())

	j2 := []byte(fakeJob())
	err = r.AddElement(util.Thens(past), "1239712984", j2)
	assert.NoError(t, err)
	assert.Equal(t, 2, r.Size())

	current := time.Now()
	err = r.AddElement(util.Thens(current.Add(10*time.Second)), "1239712985", []byte(fakeJob()))
	assert.NoError(t, err)
	assert.Equal(t, 3, r.Size())

	results, err := r.RemoveBefore(util.Thens(current.Add(1 * time.Second)))
	assert.NoError(t, err)
	assert.Equal(t, 1, r.Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{j1, j2}
	assert.Equal(t, values, results)
}

func BenchmarkBasicTimedSet(b *testing.B) {
	db, err := Open("bolt", "../test/bench.db")
	count := 10000
	assert.NoError(b, err)
	start := time.Now()
	for i := 0; i < count; i++ {
		err = db.Retries().AddElement(util.Thens(start.Add(time.Duration(rand.Intn(10*count))*time.Second)), fmt.Sprintf("abcdefghijk%d", i), []byte(fakeJob()))
	}
	info, err := os.Stat("../test/bench.db")
	assert.NoError(b, err)
	fmt.Printf("%d bytes, %.3f bytes per record\n", info.Size(), float64(info.Size())/float64(count))
}

func fakeJob() string {
	return `{"jid":"` + util.RandomJid() + `","created_at":1234567890123123123,"queue":"default","args":[1,2,3],"class":"SomeWorker"}`
}
