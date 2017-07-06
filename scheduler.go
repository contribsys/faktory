package worq

import (
	"encoding/json"
	"time"

	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
)

type Scheduler struct {
	Name     string
	ts       storage.SortedSet
	stopping bool
}

var (
	delay = 1 * time.Second
)

func (s *Scheduler) Run() {
	for {
		elms, err := s.ts.RemoveBefore(util.Nows())
		if err == nil {
			for _, elm := range elms {
				var job Job
				err := json.Unmarshal(elm, &job)
				if err != nil {
					util.Error("Unable to unmarshal json", err, elm)
					continue
				}
				q := LookupQueue(job.Queue)
				q.Push(&job)
				util.Info(s.Name, "enqueuing", job.Jid)
			}
		}
		time.Sleep(delay)
		if s.stopping {
			break
		}
	}
}

func (s *Scheduler) Stop() {
	s.stopping = true
}

func (s *Server) StartScheduler() error {
	util.Info("Starting schedulers")
	sched := &Scheduler{Name: "Scheduled", ts: s.store.Scheduled()}
	go sched.Run()
	retry := &Scheduler{Name: "Retries", ts: s.store.Retries()}
	go retry.Run()
	working := &Scheduler{Name: "Working", ts: s.store.Working()}
	go working.Run()
	return nil
}
