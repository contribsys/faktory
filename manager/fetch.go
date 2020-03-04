package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

var (
	// We can't pass a nil across the Fetcher interface boundary so we'll
	// use this sentinel value to mean nil.
	Nothing Lease = &simpleLease{}
)

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
restart:
	lease, err := m.fetcher.Fetch(ctx, wid, queues...)
	if err != nil {
		return nil, err
	}

	if lease != Nothing {
		job, err := lease.Job()
		if err != nil {
			return nil, err
		}
		err = callMiddleware(m.fetchChain, Ctx{ctx, job, m, nil}, func() error {
			return m.reserve(wid, lease)
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
			return nil, err
		}
		el.job = &job
	}

	return el.job, nil
}

func BasicFetcher(r *redis.Client) Fetcher {
	return &BasicFetch{r: r}
}

func (f *BasicFetch) Fetch(ctx context.Context, wid string, queues ...string) (Lease, error) {
	data, err := brpop(f.r, queues...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return &simpleLease{payload: data}, nil
	}
	return Nothing, nil
}

func brpop(r *redis.Client, queues ...string) ([]byte, error) {
	val, err := r.BRPop(2*time.Second, queues...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(val[1]), nil
}
