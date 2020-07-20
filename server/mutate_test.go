package server

import (
	"testing"
	"time"

	faktory "github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestMutateCommands(t *testing.T) {
	runServer(":7419", func() {
		cl, err := faktory.Open()
		assert.NoError(t, err)
		nfo, err := cl.Info()
		assert.NoError(t, err)
		assert.NotNil(t, nfo)

		err = cl.Clear(faktory.Retries)
		assert.NoError(t, err)

		hash, err := cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Scheduled"].(map[string]interface{})["size"])

		j := faktory.NewJob("AnotherJob", "truid:67123", 3)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		j = faktory.NewJob("SomeJob", "truid:67123", 3)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		j = faktory.NewJob("SomeJob", "trid:67123", 5)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		j = faktory.NewJob("FooJob", "445", 5)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)
		targetJid := j.Jid

		j = faktory.NewJob("FooJob", "445", 5)
		err = cl.Push(j)
		assert.NoError(t, err)

		hash, err = cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 4, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Scheduled"].(map[string]interface{})["size"])

		err = cl.Discard(faktory.Scheduled, faktory.OfType("SomeJob").Matching("*uid:67123*"))
		assert.NoError(t, err)

		hash, err = cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 3, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Scheduled"].(map[string]interface{})["size"])
		assert.EqualValues(t, 0, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Dead"].(map[string]interface{})["size"])

		err = cl.Kill(faktory.Scheduled, faktory.OfType("AnotherJob"))
		assert.NoError(t, err)

		err = cl.Kill("", faktory.OfType("AnotherJob"))
		assert.Error(t, err)

		hash, err = cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 2, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Scheduled"].(map[string]interface{})["size"])
		assert.EqualValues(t, 1, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Dead"].(map[string]interface{})["size"])
		assert.EqualValues(t, 1, hash["faktory"].(map[string]interface{})["queues"].(map[string]interface{})["default"])

		err = cl.Requeue(faktory.Scheduled, faktory.WithJids(targetJid))
		assert.NoError(t, err)

		hash, err = cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Scheduled"].(map[string]interface{})["size"])
		assert.EqualValues(t, 2, hash["faktory"].(map[string]interface{})["queues"].(map[string]interface{})["default"])
		assert.EqualValues(t, 1, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Dead"].(map[string]interface{})["size"])

		err = cl.Clear(faktory.Dead)
		assert.NoError(t, err)
		hash, err = cl.Info()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, hash["faktory"].(map[string]interface{})["tasks"].(map[string]interface{})["Dead"].(map[string]interface{})["size"])

	})
}
