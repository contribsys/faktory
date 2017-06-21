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

	db, err := OpenStore("../test/timed.db")
	assert.NoError(t, err)
	j1 := []byte(fakeJob())

	past := time.Now()

	assert.Equal(t, 0, db.Retries().Size())
	err = db.Retries().AddElement(util.Thens(past), "1239712983", j1)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.Retries().Size())

	j2 := []byte(fakeJob())
	err = db.Retries().AddElement(util.Thens(past), "1239712984", j2)
	assert.NoError(t, err)
	assert.Equal(t, 2, db.Retries().Size())

	current := time.Now()
	err = db.Retries().AddElement(util.Thens(current.Add(10*time.Second)), "1239712985", []byte(fakeJob()))
	assert.NoError(t, err)
	assert.Equal(t, 3, db.Retries().Size())

	results, err := db.Retries().RemoveBefore(util.Thens(current.Add(1 * time.Second)))
	assert.NoError(t, err)
	assert.Equal(t, 1, db.Retries().Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{j1, j2}
	assert.Equal(t, values, results)
}

func BenchmarkBasicTimedSet(b *testing.B) {
	db, err := OpenStore("../test/bench.db")
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

//func TestGob(t *testing.T) {
//job := fakeJob()
//hash, err := util.ParseJob([]byte(job))
//assert.NoError(t, err)
//var buf bytes.Buffer
//fmt.Println(hash)
//enc := gob.NewEncoder(&buf)
//err = enc.Encode(hash)
//assert.NoError(t, err)
//fmt.Println(len(job), len(buf.Bytes()))
//fmt.Printf("%s", buf.Bytes())
//}

//func TestTimestampFormat(t *testing.T) {
//tstamp := time.Now()
//jid := "aksdfask"
//key := fmt.Sprintf("%.10d|%s", tstamp.Unix(), jid)

//fmt.Println(key)
//}

func fakeJob() string {
	return `{"jid":"` + util.RandomJid() + `","created_at":1234567890123123123,"queue":"default","args":[1,2,3],"class":"SomeWorker"}`
}
