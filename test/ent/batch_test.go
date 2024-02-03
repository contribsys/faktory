package tester

import (
	"os"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

// Since tests are run in parallel, better use test functions names (at least)
// as queue names for isolation
func localJob(cl *client.Client, jobtype string, q string) *client.Job {
	var j = client.NewJob(jobtype, "what", "ever")
	j.Queue = q
	return j
}

func TestBatches(t *testing.T) {
	var entFaktotyIntegrationTest = os.Getenv("FAKTORY_URL") != "" && os.Getenv("FAKTORY_ENT") == "true"
	if !entFaktotyIntegrationTest {
		return
	}

	// connect to Faktory
	cl, connectError := client.Dial(client.DefaultServer(), "")
	assert.NoError(t, connectError, "Faktory server should be running and accepting connections.")

	// create a batch
	batch, createBatchError := cl.BatchNew(&client.Batch{
		Description: "We will try to do some weird stuff with this batch and see what happens.",
		Complete:    localJob(cl, "OnParentComplete", "TestBatches_ParentCallbacksQueue"),
		Success:     localJob(cl, "OnParentSuccess", "TestBatches_ParentCallbacksQueue"),
	})

	// push a couple of jobs but do not commit just
	assert.NoError(t, createBatchError, "Batch should be registered just fine by Ent (!?) Faktory.")
	batch.Push(localJob(cl, "Common", "TestBatches"))
	batch.Push(localJob(cl, "Common", "TestBatches"))

	// let's try to open a batch we have never committed ...
	openedBatch, openError := cl.BatchOpen(batch.Bid)
	// .. no error from the server (somehting not mentioned in the docs, just checking)
	assert.NoError(t, openError, "We can open a batch that we have not committed.")

	// ok, let's commit the batch now
	assert.NoError(t, batch.Commit(), "Batch committed (and now cannot be pushed into from outside).")

	// From the docs:
	// """Note that, once committed, only a job within the batch may reopen it.
	// Faktory will return an error if you dynamically add jobs from "outside" the batch;
	// this is to prevent a race condition between callbacks firing and an outsider adding more jobs."""
	// Ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#batch-open-bid

	// let's try to open an already committed batch
	reOpenedBatch, reOpenError := cl.BatchOpen(openedBatch.Bid)
	assert.NoError(t, reOpenError, "We can re-open an already committed batch from outside.")

	// let's push some extra jobs from outside and re-commit the batch
	assert.NoError(t, reOpenedBatch.Push(localJob(cl, "Extra", "TestBatches")), "Extra job can be pushed from outside.")
	assert.NoError(t, reOpenedBatch.Push(localJob(cl, "Extra", "TestBatches")), "Extra job can be pushed from outside.")
	assert.NoError(t, reOpenedBatch.Commit(), "Batch can be re-committed from outside.")

	// let's open this batch once again, add a nested batch, and commit
	reOpenedAgainBatch, reOpenAgainError := cl.BatchOpen(reOpenedBatch.Bid)
	assert.NoError(t, reOpenAgainError, "We can re-open the batch yet again.")
	nestedBatch, createNestedBatchError := cl.BatchNew(&client.Batch{
		Description: "We will try to do some weird stuff with this batch as well",
		ParentBid:   reOpenedAgainBatch.Bid,
		Complete:    localJob(cl, "OnNestedComplete", "TestBatches_CallbacksQueue"),
		Success:     localJob(cl, "OnNestedSuccess", "TestBatches_CallbacksQueue"),
	})
	assert.NoError(t, createNestedBatchError, "Can create a nested batch.")
	assert.NoError(t, nestedBatch.Push(localJob(cl, "NestedJob", "TestBatches_Nested")), "Job can be pushed into nested batch from outside.")
	assert.NoError(t, nestedBatch.Push(localJob(cl, "NestedJob", "TestBatches_Nested")), "Job can be pushed into nested batch from outside.")
	assert.NoError(t, nestedBatch.Commit(), "Nested batch can be committed from outside.")
	assert.NoError(t, reOpenedAgainBatch.Commit(), "Again committed 3rd time.")

	// check nested batch status
	batchStatus, getStatusError := cl.BatchStatus(nestedBatch.Bid)
	assert.NoError(t, getStatusError, "Retrieved nested batch status just fine")
	assert.Equal(t, int64(2), batchStatus.Total)
	assert.Equal(t, int64(2), batchStatus.Pending)
	assert.Equal(t, "", batchStatus.CompleteState) // pending
	assert.Equal(t, "", batchStatus.SuccessState)  // pending

	// From the docs:
	// """Once a callback has enqueued for a batch, you may not add anything to the batch."""
	// ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#guarantees

	// Let's consume the jobs from the out nested batch and make Faktory fire those
	// "OnNestedComplete" callback and "OnNestedSuccess" callback

	// job 1
	j, fetchErr := cl.Fetch("TestBatches_Nested")
	assert.NoError(t, fetchErr, "fetched ok")
	assert.Equal(t, "NestedJob", j.Type, "expected a job of type 'NestedJob'")
	cl.Ack(j.Jid)

	// job 2
	j, fetchErr = cl.Fetch("TestBatches_Nested")
	assert.NoError(t, fetchErr, "fetched ok")
	assert.Equal(t, "NestedJob", j.Type, "expected a job of type 'NestedJob'")
	cl.Ack(j.Jid)

	// let Faktory enqueue the callbacks
	time.Sleep(time.Duration(2) * time.Second)

	// check nested batch status again
	batchStatus, getStatusError = cl.BatchStatus(nestedBatch.Bid)
	assert.NoError(t, getStatusError, "Retrieved nested batch status just fine")
	assert.Equal(t, int64(2), batchStatus.Total)
	assert.Equal(t, int64(0), batchStatus.Pending) // job 1 and job 2 consumed!
	assert.Equal(t, int64(0), batchStatus.Failed)
	assert.Equal(t, "1", batchStatus.CompleteState) // enqueued
	assert.Equal(t, "", batchStatus.SuccessState)   // still pending

	// let's consume the "complete" callback ...
	j, fetchErr = cl.Fetch("TestBatches_CallbacksQueue")
	assert.NoError(t, fetchErr, "fetched ok")
	assert.Equal(t, "OnNestedComplete", j.Type, "expected a job of type 'OnNestedComplete'")
	cl.Ack(j.Jid)

	// ... and check the status again
	batchStatus, getStatusError = cl.BatchStatus(nestedBatch.Bid)
	assert.NoError(t, getStatusError, "Retrieved nested batch status just fine")
	assert.Equal(t, int64(2), batchStatus.Total)
	assert.Equal(t, int64(0), batchStatus.Pending)
	assert.Equal(t, int64(0), batchStatus.Failed)
	assert.Equal(t, "2", batchStatus.CompleteState) // successfully completed
	assert.Equal(t, "1", batchStatus.SuccessState)  // enqueued

	// Final touch:
	// Now with the callbacks enqueued, let's re-open the nested batch, and
	// try to push some more jobs and commit
	reOpenedNestedBatch, nestedReOpenError := cl.BatchOpen(batchStatus.Bid)
	assert.NoError(t, nestedReOpenError, "We can re-open a batch for which the callbacks have already fired.")
	assert.NoError(t, reOpenedNestedBatch.Push(localJob(cl, "DoesNotMatter", "TestBatches_Nested_2")), "Still can push.")
	assert.NoError(t, reOpenedNestedBatch.Push(localJob(cl, "DoesNotMatter", "TestBatches_Nested_2")), "Still can push")
	assert.NoError(t, reOpenedNestedBatch.Commit(), "And can also commit without error")
}
