package tester

import (
	"os"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestBatches(t *testing.T) {
	var entFaktotyIntegrationTest = os.Getenv("FAKTORY_URL") != "" && os.Getenv("FAKTORY_ENT") == "1"
	if !entFaktotyIntegrationTest {
		return
	}

	// connect to Faktory
	cl, connectError := client.Dial(client.DefaultServer(), "")
	assert.NoError(t, connectError, "Faktory server should be running and accepting connections.")

	// create a batch
	batch, createBatchError := cl.BatchNew(&client.Batch{
		Description: "We will try to do some weird stuff with this batch and see what happens.",
	})

	// push a couple of jobs and commit
	assert.NoError(t, createBatchError, "Batch should be registered just fine by Ent (!?) Faktory")
	batch.Push(client.NewJob("Common"))
	batch.Push(client.NewJob("Common"))
	commitBatchError := batch.Commit()
	assert.NoError(t, commitBatchError, "Batch committed (and now cannot be pushed into from outside)")
}
