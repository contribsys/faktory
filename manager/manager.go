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
	"github.com/go-redis/redis"
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

	AddMiddleware(fntype string, fn MiddlewareFunc)

	KV() storage.KV
	Redis() *redis.Client
}

func NewManager(s storage.Store) Manager {
	m := &manager{
		store:      s,
		workingMap: map[string]*Reservation{},
		pushChain:  make(MiddlewareChain, 0),
		failChain:  make(MiddlewareChain, 0),
		ackChain:   make(MiddlewareChain, 0),
		fetchChain: make(MiddlewareChain, 0),
	}
	m.loadWorkingSet()
	return m
}

func (m *manager) KV() storage.KV {
	return m.store.Raw()
}

func (m *manager) Redis() *redis.Client {
	return m.store.Redis()
}

func (m *manager) AddMiddleware(fntype string, fn MiddlewareFunc) {
	switch fntype {
	case "push":
		m.pushChain = append(m.pushChain, fn)
	case "ack":
		m.ackChain = append(m.ackChain, fn)
	case "fail":
		m.failChain = append(m.failChain, fn)
	case "fetch":
		m.fetchChain = append(m.fetchChain, fn)
	default:
		panic(fmt.Sprintf("Unknown middleware type: %s", fntype))
	}
}

type manager struct {
	store storage.Store

	// Hold the working set in memory so we don't need to burn CPU
	// when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove stored entry quickly.
	workingMap   map[string]*Reservation
	workingMutex sync.RWMutex
	pushChain    MiddlewareChain
	fetchChain   MiddlewareChain
	failChain    MiddlewareChain
	ackChain     MiddlewareChain
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
	if job.ReserveFor > 86400 {
		return fmt.Errorf("Jobs cannot be reserved for more than one day")
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}

	if job.Queue == "" {
		job.Queue = "default"
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
			return m.store.Scheduled().AddElement(job.At, job.Jid, data)
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

	return callMiddleware(m.pushChain, Ctx{context.Background(), job, m}, func() error {
		job.EnqueuedAt = util.Nows()
		data, err := json.Marshal(job)
		if err != nil {
			return err
		}
		//util.Debugf("pushed: %+v", job)
		return q.Push(data)
	})
}

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
restart:
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
			err = callMiddleware(m.fetchChain, Ctx{ctx, &job, m}, func() error {
				return m.reserve(wid, &job)
			})
			if h, ok := err.(halt); ok {
				// middleware halted the fetch, for whatever reason
				util.Infof("JID %s: %s", job.Jid, h.Error())
				goto restart
			}
			if err != nil {
				return nil, err
			}
			return &job, nil
		}
		if idx == 0 {
			first = q
		}
	}

	if first == nil {
		return nil, fmt.Errorf("Fetch must be called with one or more queue names")
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
		err = callMiddleware(m.fetchChain, Ctx{ctx, &job, m}, func() error {
			return m.reserve(wid, &job)
		})
		if h, ok := err.(halt); ok {
			// middleware halted the fetch, for whatever reason
			util.Debugf("JID %s: %s", job.Jid, h.Error())
			goto restart
		}
		if err != nil {
			return nil, err
		}
		return &job, nil
	}

	return nil, nil
}
