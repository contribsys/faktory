package storage

import (
	"fmt"
	"testing"

	"github.com/mperham/worq/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicTimedSet(t *testing.T) {
	t.Parallel()

	db, err := OpenStore("../test/timed.db")
	assert.NoError(t, err)
	j1 := []byte(fakeJob())

	assert.Equal(t, 0, db.Retries().Size())
	err = db.Retries().Add(1234567890, "1239712983", j1)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.Retries().Size())

	j2 := []byte(fakeJob())
	err = db.Retries().Add(1234567890, "1239712984", j2)
	assert.NoError(t, err)
	assert.Equal(t, 2, db.Retries().Size())

	fmt.Println(fmt.Sprintf("%s %s", j1, j2))
	err = db.Retries().Add(1234567895, "1239712984", []byte(fakeJob()))
	assert.NoError(t, err)
	assert.Equal(t, 3, db.Retries().Size())

	results, err := db.Retries().RemoveBefore(1234567894)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.Retries().Size())
	assert.Equal(t, 2, len(results))
	values := [][]byte{j1, j2}
	assert.Equal(t, values, results)
}

func fakeJob() string {
	return `{"jid":"` + util.RandomJid() + `","created_at":1234567890.123,"queue":"default","args":[1,2,3]}`
}
