package server

import (
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestAcknowledge(t *testing.T) {
	t.Parallel()

	set := &fakeSet{}

	job, err := acknowledge("", &fakeSet{})
	assert.NoError(t, err)
	assert.Nil(t, job)

	wid := "123876"
	job = faktory.NewJob("sometest", 1, 2, 3)

	err = reserve(wid, job, set)
	assert.NoError(t, err)

	jid := job.Jid
	job, err = acknowledge(jid, set)
	assert.NoError(t, err)
	assert.Equal(t, jid, job.Jid)

	job, err = acknowledge(jid, set)
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestReservation(t *testing.T) {
	t.Parallel()

	defer os.RemoveAll("/tmp/reservations.db")

	store, err := storage.Open("rocksdb", "/tmp/reservations.db")
	assert.NoError(t, err)
	defer store.Close()

	wid := "123876"
	job := faktory.NewJob("sometest", 1, 2, 3)

	err = reserve(wid, job, store.Working())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), store.Working().Size())

	exp := time.Now().Add(time.Duration(10) * time.Second)
	count, err := reapLongRunningJobs(store, util.Thens(exp))
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	exp = time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
	count, err = reapLongRunningJobs(store, util.Thens(exp))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

type fakeSet struct {
}

func (fs *fakeSet) AddElement(ts string, jid string, data []byte) error {
	return nil
}

func (fs *fakeSet) RemoveElement(ts string, jid string) error {
	return nil
}
