package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

const (
	// Jobs will be reserved for 30 minutes by default.
	// You can customize this per-job with the reserve_for attribute
	// in the job payload.
	DefaultTimeout = 30 * 60

	// Save dead jobs for 180 days, after that they will be purged
	DeadTTL = 180 * 24 * time.Hour
)

type Manager interface {
	Push(job *client.Job) error

	// Dispatch operations:
	//
	//  - Basic dequeue
	//    - Connection sends FETCH q1, q2
	// 	 - Job moved from Queue into Working
	//  - Scheduled
	//  	 - Job Pushed into Queue
	// 	 - Job moved from Queue into Working
	//  - Failure
	//    - Job Pushed into Retries
	//  - Push
	//    - Job Pushed into Queue
	//  - Ack
	//    - Job removed from Working
	//
	// How are jobs passed to waiting workers?
	//
	// Socket sends "FETCH q1, q2, q3"
	// Connection pops each queue:
	//   store.GetQueue("q1").Pop()
	// and returns if it gets any non-nil data.
	//
	// If all nil, the connection registers itself, blocking for a job.
	Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error)

	Acknowledge(jid string) (*client.Job, error)

	Fail(fail *FailPayload) error

	WorkingCount() int

	ReapExpiredJobs(timestamp string) (int, error)

	// Purge deletes all dead jobs
	Purge() (int64, error)

	// EnqueueScheduledJobs enqueues scheduled jobs
	EnqueueScheduledJobs() (int64, error)

	// RetryJobs enqueues failed jobs
	RetryJobs() (int64, error)

	BusyCount(wid string) int
}

func NewManager(s storage.Store) Manager {
	m := &manager{
		store:      s,
		workingMap: map[string]*Reservation{},
	}
	m.loadWorkingSet()
	return m
}

type manager struct {
	store storage.Store

	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Rocks and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Rocks entry quickly.
	workingMap   map[string]*Reservation
	workingMutex sync.RWMutex
}

func (m *manager) Push(job *client.Job) error {
	if job.Jid == "" || len(job.Jid) < 8 {
		return fmt.Errorf("All jobs must have a reasonable jid parameter")
	}
	if job.Type == "" {
		return fmt.Errorf("All jobs must have a jobtype parameter")
	}
	if job.Args == nil {
		return fmt.Errorf("All jobs must have an args parameter")
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}

	if job.Queue == "" {
		job.Queue = "default"
	}

	// Priority can never be negative because of signedness
	if job.Priority > 9 || job.Priority == 0 {
		job.Priority = 5
	}

	if job.At != "" {
		t, err := util.ParseTime(job.At)
		if err != nil {
			return fmt.Errorf("Invalid timestamp for 'at': '%s'", job.At)
		}

		if t.After(time.Now()) {
			data, err := json.Marshal(job)
			if err != nil {
				return err
			}

			// scheduler for later
			err = m.store.Scheduled().AddElement(job.At, job.Jid, data)
			if err != nil {
				return err
			}
			return nil
		}
	}

	// enqueue immediately
	return m.enqueue(job)
}

func (m *manager) enqueue(job *client.Job) error {
	q, err := m.store.GetQueue(job.Queue)
	if err != nil {
		return err
	}

	job.EnqueuedAt = util.Nows()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = q.Push(job.Priority, data)
	if err != nil {
		return err
	}

	return nil
}

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
	var first storage.Queue

	for idx, qname := range queues {
		q, err := m.store.GetQueue(qname)
		if err != nil {
			return nil, err
		}

		data, err := q.Pop()
		if err != nil {
			return nil, err
		}
		if data != nil {
			var job client.Job
			err = json.Unmarshal(data, &job)
			if err != nil {
				return nil, err
			}
			err = m.reserve(wid, &job)
			if err != nil {
				return nil, err
			}
			return &job, nil
		}
		if idx == 0 {
			first = q
		}
	}

	// scanned through our queues, no jobs were available
	// we should block for a moment, awaiting a job to be
	// pushed.  this allows us to pick up new jobs in µs
	// rather than seconds.
	data, err := first.BPop(ctx)
	if err != nil {
		return nil, err
	}
	if data != nil {
		var job client.Job
		err = json.Unmarshal(data, &job)
		if err != nil {
			return nil, err
		}
		err = m.reserve(wid, &job)
		if err != nil {
			return nil, err
		}
		return &job, nil
	}

	return nil, nil
}
