package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

func (m *manager) Fetch(ctx context.Context, wid string, queues ...string) (*client.Job, error) {
restart:
	data, err := m.fetcher.fetch(wid, queues...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		var job client.Job
		err = json.Unmarshal(data, &job)
		if err != nil {
			return nil, err
		}
		err = callMiddleware(m.fetchChain, Ctx{ctx, &job, m, nil}, func() error {
			return m.reserve(wid, &job)
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
		return &job, nil
	}

	return nil, nil
}

type Fetcher interface {
	fetch(wid string, queues ...string) ([]byte, error)
}

type BasicFetch struct {
	m Manager
}

func BasicFetcher(m *manager) Fetcher {
	return &BasicFetch{m: m}
}

func (f *BasicFetch) fetch(wid string, queues ...string) ([]byte, error) {
	return brpop(f.m, queues...)
}

func brpop(m Manager, queues ...string) ([]byte, error) {
	val, err := m.Redis().BRPop(2*time.Second, queues...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return []byte(val[1]), nil
}
