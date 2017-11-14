package server

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/stretchr/testify/assert"
)

func TestFailureScenarios(t *testing.T) {
	defer os.RemoveAll("/tmp/retries.db")

	store, err := storage.Open("rocksdb", "/tmp/retries.db")
	assert.NoError(t, err)
	defer store.Close()

	err = failProcessor(store, "{}")
	assert.Error(t, err)
	err = failProcessor(store, "{\"jd\":\"1238123123\"}")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Missing JID")

	err = failProcessor(store, "{\"jid\":\"1238123123\"}")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	job := newjob(25)
	reserve("123", job, store.Working())
	assert.NotNil(t, workingMap[job.Jid])
	assert.Nil(t, workingMap[job.Jid].Job.Failure)
	assert.Equal(t, int64(0), store.Retries().Size())

	err = failProcessor(store, fp(job.Jid, "uh oh", "SomeError", nil))
	assert.NoError(t, err)
	assert.Nil(t, workingMap[job.Jid])
	assert.Equal(t, int64(1), store.Retries().Size())
}

func fp(jid, msg, errtype string, bt []string) string {
	var fail FailPayload
	fail.Jid = jid
	fail.ErrorMessage = msg
	fail.ErrorType = errtype
	fail.Backtrace = bt
	data, err := json.Marshal(&fail)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func newjob(retries int) *client.Job {
	j := client.NewJob("RetryJob", 1, 2, 3)
	j.Retry = retries
	return j
}
