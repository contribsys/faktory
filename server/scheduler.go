package server

import (
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/storage"
	"github.com/mperham/faktory/util"
)

type Scheduler struct {
	Name     string
	adapter  SchedulerAdapter
	stopping chan interface{}
	delay    time.Duration

	jobs     int64
	walltime int64
	cycles   int64
}

var (
	defaultDelay = 1 * time.Second
)

func (s *Scheduler) cycle() int64 {
	count := int64(0)
	start := time.Now()
	elms, err := s.adapter.Prune(util.Nows())
	if err == nil {
		if len(elms) > 0 {
			util.Infof("%s enqueueing %d jobs", s.Name, len(elms))
		}

		for _, elm := range elms {
			var job faktory.Job
			err := json.Unmarshal(elm, &job)
			if err != nil {
				util.Error("Unable to unmarshal json", err, elm)
				continue
			}
			err = s.adapter.Push(job.Queue, elm)
			if err != nil {
				util.Warnf("Error pushing job to '%s': %s", job.Queue, err.Error())
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

type rocksAdapter struct {
	store   storage.Store
	ts      storage.SortedSet
	goodbye bool
}

func (ra *rocksAdapter) Push(name string, elm []byte) error {
	if ra.goodbye {
		// when the Dead elements come up for "scheduling", they have
		// expired and are removed forever.  Goodbye!
		util.Debug("Removing dead element forever.  Goodbye.")
		return nil
	}

	que, err := ra.store.GetQueue(name)
	if err != nil {
		return err
	}
	return que.Push(elm)
}
func (ra *rocksAdapter) Prune(string) ([][]byte, error) {
	return ra.ts.RemoveBefore(util.Nows())
}

type SchedulerAdapter interface {
	Prune(string) ([][]byte, error)
	Push(string, []byte) error
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
			s.cycle()
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
		"enqueued":      s.jobs,
		"cycles":        s.cycles,
		"wall_time_sec": (float64(s.walltime) / 1000000000),
	}
}

func (s *Scheduler) Stop() {
	close(s.stopping)
}

func NewScheduler(name string, adapter SchedulerAdapter) *Scheduler {
	return &Scheduler{
		Name:     name,
		adapter:  adapter,
		stopping: make(chan interface{}),
		delay:    defaultDelay,
	}
}

type SchedulerSubsystem struct {
	Retries   *Scheduler
	Working   *Scheduler
	Scheduled *Scheduler
	Dead      *Scheduler
}

func (ss *SchedulerSubsystem) Stop() {
	util.Info("Stopping scheduler subsystem")

	ss.Retries.Stop()
	ss.Working.Stop()
	ss.Scheduled.Stop()
	ss.Dead.Stop()
}

func (s *Server) StartScheduler(waiter *sync.WaitGroup) *SchedulerSubsystem {
	util.Info("Starting scheduler subsystem")

	ss := &SchedulerSubsystem{
		Scheduled: NewScheduler("Scheduled", &rocksAdapter{s.store, s.store.Scheduled(), false}),
		Retries:   NewScheduler("Retries", &rocksAdapter{s.store, s.store.Retries(), false}),
		Working:   NewScheduler("Working", &rocksAdapter{s.store, s.store.Working(), false}),
		Dead:      NewScheduler("Dead", &rocksAdapter{s.store, s.store.Dead(), true}),
	}

	ss.Scheduled.Run(waiter)
	ss.Retries.Run(waiter)
	ss.Working.Run(waiter)
	ss.Dead.Run(waiter)
	s.scheduler = ss
	return ss
}
