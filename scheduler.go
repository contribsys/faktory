package worq

import (
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
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

	jobs     int64
	walltime int64
	cycles   int64
}

var (
	defaultDelay = 1 * time.Second
)

func (s *Scheduler) Cycle() int {
	count := 0
	start := time.Now()
	elms, err := s.ts.RemoveBefore(util.Nows())
	if err == nil {
		if len(elms) > 0 {
			util.Infof("%s enqueueing %d jobs", s.Name, len(elms))
		}

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

			count += 1
		}
	}
	end := time.Now()
	atomic.AddInt64(&s.cycles, 1)
	atomic.AddInt64(&s.jobs, int64(count))
	atomic.AddInt64(&s.walltime, end.Sub(start).Nanoseconds())
	return count
}

func (s *Scheduler) Run(waiter *sync.WaitGroup) {
	go func() {
		waiter.Add(1)
		defer waiter.Done()

		// add random jitter so all scheduler goroutines don't all fire at the same µs
		time.Sleep(time.Duration(rand.Float64()) * time.Second)
		timer := time.NewTicker(s.delay)
		defer timer.Stop()

		for {
			s.Cycle()
			select {
			case <-timer.C:
			case <-s.stopping:
				return
			}
		}
	}()
}

func (s *Scheduler) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":          s.ts.Size(),
		"enqueued":      s.jobs,
		"cycles":        s.cycles,
		"wall_time_sec": (float64(s.walltime) / 1000000000),
	}
}

func (s *Scheduler) Stop() {
	close(s.stopping)
}

func NewScheduler(name string, store storage.Store, set storage.SortedSet) *Scheduler {
	return &Scheduler{Name: name, store: store, ts: set, stopping: make(chan interface{}), delay: defaultDelay}
}

type SchedulerSubsystem struct {
	Retries   *Scheduler
	Working   *Scheduler
	Scheduled *Scheduler
	waiter    *sync.WaitGroup
}

func (ss *SchedulerSubsystem) Stop() {
	util.Info("Stopping scheduler subsystem")

	ss.Retries.Stop()
	ss.Working.Stop()
	ss.Scheduled.Stop()
	ss.waiter.Wait()
}

func (s *Server) StartScheduler() *SchedulerSubsystem {
	util.Info("Starting scheduler subsystem")

	ss := &SchedulerSubsystem{
		Scheduled: NewScheduler("Scheduled", s.store, s.store.Scheduled()),
		Retries:   NewScheduler("Retries", s.store, s.store.Retries()),
		Working:   NewScheduler("Working", s.store, s.store.Working()),
		waiter:    &sync.WaitGroup{},
	}

	ss.Scheduled.Run(ss.waiter)
	ss.Retries.Run(ss.waiter)
	ss.Working.Run(ss.waiter)
	return ss
}
