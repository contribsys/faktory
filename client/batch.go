package client

import (
	"encoding/json"
	"fmt"

	"github.com/contribsys/faktory/util"
)

type BatchStatus struct {
	Bid         string `json:"bid"`
	ParentBid   string `json:"parent_bid,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   string `json:"created_at"`
	Total       int64  `json:"total"`
	Pending     int64  `json:"pending"`
	Failed      int64  `json:"failed"`

	// "" if pending,
	// "1" if callback enqueued,
	// "2" if callback finished successfully
	CompleteState string `json:"complete_st"`
	SuccessState  string `json:"success_st"`
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

// Allocate a new Batch.
// Caller must set one or more callbacks and
// push one or more jobs in the batch.
//
//	b := faktory.NewBatch(cl)
//	b.Success = faktory.NewJob("MySuccessCallback", 12345)
//	b.Jobs(func() error {
//	  b.Push(...)
//	})
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
		if _, err := b.faktory.BatchNew(b); err != nil {
			return fmt.Errorf("cannot create new batch: %w", err)
		}
	}
	if b.faktory == nil || b.committed {
		return ErrBatchNotOpen
	}

	if err := fn(); err != nil {
		return fmt.Errorf("cannot push jobs in the %q batch: %w", b.Bid, err)
	}

	return b.Commit()
}

func (b *Batch) Push(job *Job) error {
	if b.new {
		return ErrBatchNotOpen
	}
	if b.faktory == nil || b.committed {
		return ErrBatchAlreadyCommitted
	}
	job.SetCustom("bid", b.Bid)
	return b.faktory.Push(job)
}

// Result is map[JID]ErrorMessage
func (b *Batch) PushBulk(jobs []*Job) (map[string]string, error) {
	if b.new {
		return nil, ErrBatchNotOpen
	}
	if b.faktory == nil || b.committed {
		return nil, ErrBatchAlreadyCommitted
	}

	for _, job := range jobs {
		job.SetCustom("bid", b.Bid)
	}

	return b.faktory.PushBulk(jobs)
}

// Commit any pushed jobs in the batch to Redis so they can fire callbacks.
// A Batch object can only be committed once.
// You must use client.BatchOpen to get a new copy if you want to commit more jobs.
func (b *Batch) Commit() error {
	if b.new {
		return ErrBatchNotOpen
	}
	if b.faktory == nil || b.committed {
		return ErrBatchAlreadyCommitted
	}
	if err := b.faktory.BatchCommit(b.Bid); err != nil {
		return fmt.Errorf("cannot commit %q batch: %w", b.Bid, err)
	}

	b.faktory = nil
	b.committed = true

	return nil
}

var (
	ErrBatchAlreadyCommitted = fmt.Errorf("batch has already been committed, must reopen")
	ErrBatchNotOpen          = fmt.Errorf("batch must be opened before it can be used")
)

/////////////////////////////////////////////
// Low-level command API

func (c *Client) BatchCommit(bid string) error {
	err := c.writeLine(c.wtr, "BATCH COMMIT", []byte(bid))
	if err != nil {
		return err
	}

	return c.ok(c.rdr)
}

func (c *Client) BatchNew(def *Batch) (*Batch, error) {
	if def.Bid != "" {
		return nil, fmt.Errorf("BID must be blank when creating a new Batch, cannot specify it")
	}
	bbytes, err := json.Marshal(def)
	if err != nil {
		return nil, err
	}

	err = c.writeLine(c.wtr, "BATCH NEW", bbytes)
	if err != nil {
		return nil, err
	}

	bid, err := c.readString(c.rdr)
	if err != nil {
		return nil, err
	}
	def.Bid = bid
	def.new = false
	def.faktory = c
	return def, nil
}

func (c *Client) BatchStatus(bid string) (*BatchStatus, error) {
	err := c.writeLine(c.wtr, "BATCH STATUS", []byte(bid))
	if err != nil {
		return nil, err
	}

	data, err := c.readResponse(c.rdr)
	if err != nil {
		return nil, err
	}

	var stat BatchStatus
	err = util.JsonUnmarshal(data, &stat)
	if err != nil {
		return nil, err
	}

	return &stat, nil
}

func (c *Client) BatchOpen(bid string) (*Batch, error) {
	err := c.writeLine(c.wtr, "BATCH OPEN", []byte(bid))
	if err != nil {
		return nil, err
	}

	bbid, err := c.readString(c.rdr)
	if err != nil {
		return nil, err
	}
	b := &Batch{
		Bid:     bbid,
		new:     false,
		faktory: c,
	}
	return b, nil
}
