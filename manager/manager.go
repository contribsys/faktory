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
	"github.com/redis/go-redis/v9"
)

const (
	// Jobs will be reserved for 30 minutes by default.
	// You can customize this per-job with the reserve_for attribute
	// in the job payload.
	DefaultTimeout = 30 * 60

	// Save dead jobs for 180 days, after that they will be purged
	DeadTTL = 180 * 24 * time.Hour
)

// A KnownError is one that returns a specific error code to the client
// such that it can be handled explicitly.  For example, the unique job feature
// will return a NOTUNIQUE error when the client tries to push() a job that already
// exists in Faktory.
//
// Unexpected errors will always use "ERR" as their code, for instance any
// malformed data, network errors, IO errors, etc.  Clients are expected to
// raise an exception for any ERR response.
type KnownError interface {
	error
	Code() string
}

type codedError struct {
	code string
	msg  string
}

func (t *codedError) Error() string {
	return fmt.Sprintf("%s %s", t.code, t.msg)
}

func (t *codedError) Code() string {
	return t.code
}

func ExpectedError(code string, msg string) error {
	return &codedError{code: code, msg: msg}
}

type Manager interface {
	Push(ctx context.Context, job *client.Job) error
	// TODO PushBulk(jobs []*client.Job) map[*client.Job]error

	PauseQueue(ctx context.Context, qName string) error
	ResumeQueue(ctx context.Context, qName string) error
	RemoveQueue(ctx context.Context, qName string) error

	// Dispatch operations:
	//
	//  - Basic dequeue
	//    - Connection sends FETCH q1, q2
	//	 - Job moved from Queue into Working
	//  - Scheduled
	//	 - Job Pushed into Queue
	//	 - Job moved from Queue into Working
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

	Acknowledge(ctx context.Context, jid string) (*client.Job, error)

	Fail(ctx context.Context, fail *FailPayload) error

	// Allows arbitrary extension of a job's current reservation
	// This is a no-op if you set the time before the current
	// reservation expiry.
	ExtendReservation(ctx context.Context, jid string, until time.Time) error

	WorkingCount() int

	ReapExpiredJobs(ctx context.Context, when time.Time) (int64, error)

	// Purge deletes all dead jobs
	Purge(ctx context.Context, when time.Time) (int64, error)

	// EnqueueScheduledJobs enqueues scheduled jobs
	EnqueueScheduledJobs(ctx context.Context, when time.Time) (int64, error)

	// RetryJobs enqueues failed jobs
	RetryJobs(ctx context.Context, when time.Time) (int64, error)

	BusyCount(wid string) int

	AddMiddleware(fntype string, fn MiddlewareFunc)

	KV() storage.KV
	Redis() *redis.Client
	SetFetcher(f Fetcher)
}

func NewManager(s storage.Store) Manager {
	return newManager(s)
}

func newManager(s storage.Store) *manager {
	m := &manager{
		store:      s,
		workingMap: map[string]*Reservation{},
		pushChain:  make(MiddlewareChain, 0),
		failChain:  make(MiddlewareChain, 0),
		ackChain:   make(MiddlewareChain, 0),
		fetchChain: make(MiddlewareChain, 0),
	}
	ctx := context.Background()
	_ = m.loadWorkingSet(ctx)
	p, _ := s.PausedQueues(ctx)
	m.paused = p
	m.fetcher = BasicFetcher(m.Redis())
	return m
}

func (m *manager) SetFetcher(f Fetcher) {
	m.fetcher = f
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

type Lease interface {
	Release() error
	Payload() []byte
	Job() (*client.Job, error)
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
	fetcher      Fetcher
	paused       []string
}

func (m *manager) Push(ctx context.Context, job *client.Job) error {
	if job.Jid == "" || len(job.Jid) < 8 {
		return fmt.Errorf("jobs must have a reasonable jid parameter")
	}
	if job.Type == "" {
		return fmt.Errorf("jobs must have a jobtype parameter")
	}
	if job.Args == nil {
		return fmt.Errorf("jobs must have an args parameter")
	}
	if job.ReserveFor > 86400 {
		return fmt.Errorf("jobs cannot be reserved for more than one day")
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}

	if job.Queue == "" {
		job.Queue = "default"
	}

	var err error
	var t time.Time
	if job.At != "" {
		t, err = util.ParseTime(job.At)
		if err != nil {
			return fmt.Errorf("invalid timestamp for 'at': %q: %w", job.At, err)
		}
	}

	ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{job, m, nil})
	err = callMiddleware(ctxh, m.pushChain, func() error {
		if job.At != "" {
			if t.After(time.Now()) {
				data, err := json.Marshal(job)
				if err != nil {
					return fmt.Errorf("cannot marshal job payload: %w", err)
				}

				// scheduler for later
				return m.store.Scheduled().AddElement(ctx, job.At, job.Jid, data)
			}
		}
		return m.enqueue(ctx, job)
	})
	if err != nil {
		if k, ok := err.(KnownError); ok {
			util.Infof("JID %s: %s", job.Jid, k.Error())
		}
	}
	return err
}

func (m *manager) enqueue(ctx context.Context, job *client.Job) error {
	q, err := m.store.GetQueue(ctx, job.Queue)
	if err != nil {
		return fmt.Errorf("cannot get %q queue: %w", job.Queue, err)
	}

	job.EnqueuedAt = util.Nows()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("cannot marshal job payload: %w", err)
	}
	return q.Push(ctx, data)
}
