package worq

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/mperham/worq/storage"
	"github.com/mperham/worq/util"
)

type Scheduler struct {
	Name     string
	store    storage.Store
	ts       storage.SortedSet
	stopping chan interface{}
	delay    time.Duration
}

var (
	defaultDelay = 1 * time.Second
)

func (s *Scheduler) Cycle() int {
	count := 0
	elms, err := s.ts.RemoveBefore(util.Nows())
	if err == nil {
		for _, elm := range elms {
			var job Job
			err := json.Unmarshal(elm, &job)
			if err != nil {
				util.Error("Unable to unmarshal json", err, elm)
				continue
			}
			que, err := s.store.GetQueue(job.Queue)
			if err != nil {
				util.Warn("Error getting queue", job.Queue, err)
				continue
			}
			err = que.Push(elm)
			if err != nil {
				util.Warn("Error pushing job", job.Queue, err)
				continue
			}

			util.Info(s.Name, "enqueuing", job.Jid)
			count += 1
		}
	}
	return count
}

func (s *Scheduler) Run(waiter *sync.WaitGroup) {
	go func() {
		waiter.Add(1)
		defer waiter.Done()

		// add random jitter so all scheduler goroutines don't all fire at the same µs
		time.Sleep(time.Duration(rand.Float64()) * time.Second)
		timer := time.NewTimer(s.delay)
		defer timer.Stop()

		util.Debug(s.Name, "starting")
		for {
			s.Cycle()
			select {
			case <-timer.C:
			case <-s.stopping:
				break
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	close(s.stopping)
}

func NewScheduler(name string, store storage.Store, set storage.SortedSet) *Scheduler {
	return &Scheduler{Name: name, store: store, ts: set, stopping: make(chan interface{}), delay: defaultDelay}
}

type SchedulerSubsystem struct {
	retries   *Scheduler
	working   *Scheduler
	scheduled *Scheduler
	waiter    *sync.WaitGroup
}

func (ss *SchedulerSubsystem) Stop() {
	util.Info("Stopping scheduler subsystem")

	ss.retries.Stop()
	ss.working.Stop()
	ss.scheduled.Stop()
	ss.waiter.Wait()
}

func (s *Server) StartScheduler() *SchedulerSubsystem {
	util.Info("Starting scheduler subsystem")

	ss := &SchedulerSubsystem{
		scheduled: NewScheduler("Scheduled", s.store, s.store.Scheduled()),
		retries:   NewScheduler("Retries", s.store, s.store.Retries()),
		working:   NewScheduler("Working", s.store, s.store.Working()),
		waiter:    &sync.WaitGroup{},
	}

	ss.scheduled.Run(ss.waiter)
	ss.retries.Run(ss.waiter)
	ss.working.Run(ss.waiter)
	return ss
}
