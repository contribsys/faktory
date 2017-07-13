/*
Dispatch operations:

 - Basic dequeue
   - Connection sends POP q1, q2
	 - Job moved from Queue into Working
 - Scheduled
 	 - Job Pushed into Queue
	 - Job moved from Queue into Working
 - Failure
   - Job Pushed into Retries
 - Push
   - Job Pushed into Queue
 - Ack
   - Job removed from Working

How are jobs passed to waiting workers?

Socket sends "POP q1, q2, q3"
Connection pops each queue:
  store.GetQueue("q1").Pop()
and returns if it gets any non-nil data.

If all nil, the connection registers itself, blocking for a job.
*/
package worq

import "encoding/json"

func (s *Server) Pop(fn func(*Job) error, queues ...string) (*Job, error) {
	for _, q := range queues {
		que, err := s.store.GetQueue(q)
		if err != nil {
			return nil, err
		}
		data, err := que.Pop()
		if err != nil {
			return nil, err
		}
		if data != nil {
			var job Job
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
	}
	return nil, nil
}
