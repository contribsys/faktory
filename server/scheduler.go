package server

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type scanner struct {
	name     string
	adapter  scannerAdapter
	jobs     int64
	walltime int64
	cycles   int64
}

func (s *scanner) Name() string {
	return s.name
}

func (s *scanner) Execute() error {
	count := int64(0)
	start := time.Now()
	elms, err := s.adapter.Prune(util.Nows())
	if err != nil {
		return err
	}

	if len(elms) > 0 {
		util.Infof("%s processing %d jobs", s.name, len(elms))
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
	end := time.Now()
	atomic.AddInt64(&s.cycles, 1)
	atomic.AddInt64(&s.jobs, int64(count))
	atomic.AddInt64(&s.walltime, end.Sub(start).Nanoseconds())
	return nil
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
func (ra *rocksAdapter) Size() int64 {
	return ra.ts.Size()
}

type scannerAdapter interface {
	Prune(string) ([][]byte, error)
	Push(string, []byte) error
	Size() int64
}

func (s *scanner) Stats() map[string]interface{} {
	return map[string]interface{}{
		"enqueued":      atomic.LoadInt64(&s.jobs),
		"cycles":        atomic.LoadInt64(&s.cycles),
		"size":          s.adapter.Size(),
		"wall_time_sec": (float64(atomic.LoadInt64(&s.walltime)) / 1000000000),
	}
}

func (s *Server) startScanners(waiter *sync.WaitGroup) {
	s.taskRunner.AddTask(5, &scanner{name: "Scheduled", adapter: &rocksAdapter{s.store, s.store.Scheduled(), false}})
	s.taskRunner.AddTask(5, &scanner{name: "Retries", adapter: &rocksAdapter{s.store, s.store.Retries(), false}})
	s.taskRunner.AddTask(60, &scanner{name: "Dead", adapter: &rocksAdapter{s.store, s.store.Dead(), true}})
}
