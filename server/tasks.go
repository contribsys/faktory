package server

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory/util"
)

/*
 * The task runner allows us to run internal tasks on
 * a recurring schedule, e.g. "reap old heartbeats every 30 seconds".
 *
 *
 * tr = &TaskRunner{Server: server}
 * tr.AddTask("heartbeat reaper", server.reapHeartbeats, 30)
 */
type TaskRunner struct {
	server   *Server
	stopping chan interface{}
	tasks    []*Task

	walltimeNs int64
	cycles     int64
	executions int64
}

type Task struct {
	Name       string
	fn         func() error
	every      int64
	runs       int64
	walltimeNs int64
}

func (ts *TaskRunner) AddTask(name string, fn func() error, sec int64) {
	var task Task
	task.Name = name
	task.fn = fn
	task.every = sec
	ts.tasks = append(ts.tasks, &task)
}

func (ts *TaskRunner) Cycle() {
	count := int64(0)
	start := time.Now()
	sec := start.Unix()
	for _, t := range ts.tasks {
		if sec%t.every != 0 {
			continue
		}
		tstart := time.Now()
		err := t.fn()
		tend := time.Now()
		if err != nil {
			util.Warn("Error running task %s: %v", t.Name, err)
		}
		atomic.AddInt64(&t.runs, 1)
		atomic.AddInt64(&t.walltimeNs, tend.Sub(tstart).Nanoseconds())
		count += 1
	}
	end := time.Now()
	atomic.AddInt64(&ts.cycles, 1)
	atomic.AddInt64(&ts.executions, count)
	atomic.AddInt64(&ts.walltimeNs, end.Sub(start).Nanoseconds())
}

func (s *TaskRunner) Run(waiter *sync.WaitGroup) {
	go func() {
		waiter.Add(1)
		defer waiter.Done()

		// add random jitter so all scheduler goroutines don't all fire at the same µs
		time.Sleep(time.Duration(rand.Float64()) * time.Second)
		timer := time.NewTicker(1)
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

func (ts *TaskRunner) Stats() map[string]map[string]interface{} {
	data := map[string]map[string]interface{}{}

	for _, task := range ts.tasks {
		data[task.Name] = map[string]interface{}{
			"runs":           task.runs,
			"wall_time_usec": (task.walltimeNs / 1000000),
		}
	}
	return data
}

func (ts *TaskRunner) Stop() {
	close(ts.stopping)
}
