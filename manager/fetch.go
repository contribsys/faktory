package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

var (
	// We can't pass a nil across the Fetcher interface boundary so we'll
	// use this sentinel value to mean nil.
	Nothing Lease = &simpleLease{}
)

func (m *manager) RemoveQueue(ctx context.Context, qName string) error {
	q, ok := m.store.ExistingQueue(ctx, qName)
	if ok {
		_, err := q.Clear(ctx)
		if err != nil {
			return fmt.Errorf("cannot remove queue: %w", err)
		}
	}
	m.paused = filter([]string{qName}, m.paused)
	return nil
}

func (m *manager) PauseQueue(ctx context.Context, qName string) error {
	q, ok := m.store.ExistingQueue(ctx, qName)
	if ok {
		err := q.Pause(ctx)
		if err != nil {
			return fmt.Errorf("cannot pause queue: %w", err)
		}
		m.paused = append(filter([]string{qName}, m.paused), qName)
	}
	return nil
}

func (m *manager) ResumeQueue(ctx context.Context, qName string) error {
	q, ok := m.store.ExistingQueue(ctx, qName)
	if ok {
		err := q.Resume(ctx)
		if err != nil {
			return fmt.Errorf("cannot resume queue: %w", err)
		}

		m.paused = filter([]string{qName}, m.paused)
	}
	return nil
}

// returns the subset of "queues" which are not in "paused"
func filter(paused []string, queues []string) []string {
	if len(paused) == 0 {
		return queues
	}

	qs := make([]string, len(queues))
	count := 0

	for qidx := 0; qidx < len(queues); qidx++ {
		if !contains(queues[qidx], paused) {
			qs[count] = queues[qidx]
			count++
		}
	}
	return qs[:count]
}

func contains(a string, slc []string) bool {
	for x := range slc {
		if a == slc[x] {
			return true
		}
	}
	return false
}

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
	if len(queues) == 0 {
		return nil, fmt.Errorf("must call fetch with at least one queue")
	}

restart:
	activeQueues := filter(m.paused, queues)
	if len(activeQueues) == 0 {
		// if we pause all queues, there is nothing to fetch
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
		return nil, nil
	}

	lease, err := m.fetcher.Fetch(ctx, wid, activeQueues...)
	if err != nil {
		return nil, err
	}

	if lease != Nothing {
		job, err := lease.Job()
		if err != nil {
			return nil, err
		}
		ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{job, m, nil})
		err = callMiddleware(ctxh, m.fetchChain, func() error {
			return m.reserve(ctxh, wid, lease)
		})
		if h, ok := err.(KnownError); ok {
			util.Infof("JID %s: %s", job.Jid, h.Error())
			if h.Code() == "DISCARD" {
				goto restart
			}
			return nil, err
		}
		if err != nil {
			return nil, err
		}
		return job, nil
	}
	return nil, nil
}

type Fetcher interface {
	Fetch(ctx context.Context, wid string, queues ...string) (Lease, error)
}

type BasicFetch struct {
	r *redis.Client
}

type simpleLease struct {
	payload  []byte
	job      *client.Job
	released bool
}

func (el *simpleLease) Release() error {
	el.released = true
	return nil
}

func (el *simpleLease) Payload() []byte {
	return el.payload
}

func (el *simpleLease) Job() (*client.Job, error) {
	if el.job != nil {
		return el.job, nil
	}
	if el.payload == nil {
		return nil, nil
	}
	if el.job == nil {
		var job client.Job
		err := json.Unmarshal(el.payload, &job)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal job payload: %w", err)
		}
		el.job = &job
	}

	return el.job, nil
}

func BasicFetcher(r *redis.Client) Fetcher {
	return &BasicFetch{r: r}
}

func (f *BasicFetch) Fetch(ctx context.Context, wid string, queues ...string) (Lease, error) {
	data, err := brpop(ctx, f.r, queues...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return &simpleLease{payload: data}, nil
	}
	return Nothing, nil
}

func brpop(ctx context.Context, r *redis.Client, queues ...string) ([]byte, error) {
	val, err := r.BRPop(ctx, 2*time.Second, queues...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(val[1]), nil
}
