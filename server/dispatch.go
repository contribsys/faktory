package server

import (
	"context"
	"encoding/json"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/storage"
)

// Dispatch operations:
//
//  - Basic dequeue
//    - Connection sends POP q1, q2
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
// Socket sends "POP q1, q2, q3"
// Connection pops each queue:
//   store.GetQueue("q1").Pop()
// and returns if it gets any non-nil data.
//
// If all nil, the connection registers itself, blocking for a job.

func (s *Server) Fetch(fn func(*faktory.Job) error, ctx context.Context, queues ...string) (*faktory.Job, error) {
	var first storage.Queue

	for idx, q := range queues {
		que, err := s.store.GetQueue(q)
		if err != nil {
			return nil, err
		}
		//util.Debugf("Checking %s", que.Name())
		data, err := que.Pop()
		if err != nil {
			return nil, err
		}
		if data != nil {
			var job faktory.Job
			err = json.Unmarshal(data, &job)
			if err != nil {
				return nil, err
			}
			err = fn(&job)
			if err != nil {
				return nil, err
			}
			return &job, nil
		}
		if idx == 0 {
			first = que
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
		var job faktory.Job
		err = json.Unmarshal(data, &job)
		if err != nil {
			return nil, err
		}
		err = fn(&job)
		if err != nil {
			return nil, err
		}
		return &job, nil
	}
	//util.Debugf("Done blocking on %s", first.Name())
	return nil, nil
}
