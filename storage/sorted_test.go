package storage

import (
	"fmt"
	"testing"

	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicSortedOps(t *testing.T) {
	store, teardown := setupTest(t)
	defer teardown(t)

	sset := store.Retries()
	assert.EqualValues(t, 0, sset.Size())

	time := util.Nows()
	jid := util.RandomJid()
	err := sset.AddElement(time, jid, []byte("some data"))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, sset.Size())

	key := fmt.Sprintf("%s|%s", time, jid)
	data, err := sset.Get([]byte(key))
	assert.NoError(t, err)
	assert.Equal(t, "some data", string(data))

	// add a second job with exact same time to handle edge case of
	// sorted set entries with same score.
	newjid := util.RandomJid()
	payload := []byte(fmt.Sprintf("some data%s", newjid))
	err = sset.AddElement(time, newjid, payload)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, sset.Size())

	newkey := fmt.Sprintf("%s|%s", time, newjid)
	data, err = sset.Get([]byte(newkey))
	assert.NoError(t, err)
	assert.Equal(t, payload, data)

	err = sset.Remove([]byte(newkey))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, sset.Size())

	err = sset.RemoveElement(time, jid)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, sset.Size())

	err = sset.AddElement(time, newjid, payload)
	assert.EqualValues(t, 1, sset.Size())

	assert.Equal(t, sset.Name(), "retries")
	assert.NoError(t, sset.Clear())
	assert.EqualValues(t, 0, sset.Size())

}
