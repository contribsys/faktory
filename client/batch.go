package client

import (
	"fmt"
)

type BatchStatus struct {
	Bid         string `json:"bid"`
	ParentBid   string `json:"parent_bid,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   string `json:"created_at"`
	Total       int64  `json:"total"`
	Pending     int64  `json:"pending"`
	Failed      int64  `json:"failed"`
	Succeeded   bool   `json:"succeeded"`
	Completed   bool   `json:"completed"`
}

type Batch struct {
	// Unique identifier for each batch.
	// NB: the caller should not set this, it is generated
	// by Faktory when the batch is persisted to Redis.
	Bid string `json:"bid"`

	ParentBid   string `json:"parent_bid,omitempty"`
	Description string `json:"description,omitempty"`
	Success     *Job   `json:"success,omitempty"`
	Complete    *Job   `json:"complete,omitempty"`

	faktory   *Client
	committed bool
	new       bool
}

//
// Allocate a new Batch.
// Caller must set one or more callbacks and
// push one or more jobs in the batch.
//
//   b := faktory.NewBatch(cl)
//   b.Success = faktory.NewJob("MySuccessCallback", 12345)
//   b.Jobs(func() error {
//     b.Push(...)
//   })
//   b.Commit()
//
func NewBatch(cl *Client) *Batch {
	return &Batch{
		committed: false,
		new:       true,
		faktory:   cl,
	}
}

// Push one or more jobs within this function.
// Job processing will start **immediately**
// but callbacks will not fire until Commit()
// is called, allowing you to push jobs in slowly
// and avoid the obvious race condition.
func (b *Batch) Jobs(fn func() error) error {
	if b.new {
		_, err := b.faktory.BatchNew(b)
		if err != nil {
			return err
		}
	}
	if b.faktory == nil || b.committed {
		return BatchNotOpen
	}

	err := fn()
	if err == nil {
		return b.Commit()
	}
	return err
}

func (b *Batch) Push(job *Job) error {
	if b.new {
		return BatchNotOpen
	}
	if b.faktory == nil || b.committed {
		return BatchAlreadyCommitted
	}
	job.SetCustom("bid", b.Bid)
	return b.faktory.Push(job)
}

// Commit any pushed jobs in the batch to Redis so they can fire callbacks.
// A Batch object can only be committed once.
// You must use client.BatchOpen to get a new copy if you want to commit more jobs.
func (b *Batch) Commit() error {
	if b.new {
		return BatchNotOpen
	}
	if b.faktory == nil || b.committed {
		return BatchAlreadyCommitted
	}
	err := b.faktory.BatchCommit(b.Bid)
	b.faktory = nil
	b.committed = true
	return err
}

var (
	BatchAlreadyCommitted = fmt.Errorf("Batch has already been committed, must reopen")
	BatchNotOpen          = fmt.Errorf("Batch must be opened before it can be used")
)
