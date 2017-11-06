package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
)

//
// This represents a single worker process.  It may have many network
// connections open to Faktory.  Each worker process should send a BEAT
// command every 15 seconds.  If Faktory does not receive a BEAT from a
// worker process within 60 seconds, it expires and is removed from the
// Busy page.
//
// From Faktory's POV, the process can BEAT again and resume normal operations, e.g.
// due to a network partition.
// If a process dies, it will be removed after 1 minute and its jobs recovered after the
// job reservation timeout has passed (typically 30 minutes).
//
// A worker process has a simple three-state lifecycle:
//
//  running -> quiet -> terminate
//
// - Running means the worker is alive and processing jobs.
// - Quiet means the worker should stop FETCHing new jobs but continue working on existing jobs.
// It should not exit, even if no jobs are processing.
// - Terminate means the worker should exit within N seconds, where N is recommended to be
// 30 seconds.  In practice, faktory_worker_ruby waits up to 25 seconds and any
// threads that are still busy are forcefully killed and their associated jobs reported
// as FAILed so they will be retried shortly.
//
// A worker process should never stop sending BEAT.  Even after "quiet" or "terminate", the BEAT
// should continue, only stopping due to process exit().
// Workers should never move backward in state - you cannot "unquiet" a worker, it must be restarted.
//
// Workers will typically also respond to standard Unix signals.
// faktory_worker_ruby uses TSTP ("Threads SToP") as the quiet signal and TERM as the terminate signal.
//
type ClientWorker struct {
	Hostname     string   `json:"hostname"`
	Wid          string   `json:"wid"`
	Pid          int      `json:"pid"`
	Labels       []string `json:"labels"`
	PasswordHash string   `json:"pwdhash"`
	StartedAt    time.Time

	lastHeartbeat time.Time
	state         WorkerState
}

type WorkerState int

const (
	Running WorkerState = iota
	Quiet
	Terminate
)

func stateSignal(state WorkerState) string {
	switch state {
	case Quiet:
		return "quiet"
	case Terminate:
		return "terminate"
	default:
		return ""
	}
}

func clientWorkerFromHello(data string) (*ClientWorker, error) {
	var client ClientWorker
	err := json.Unmarshal([]byte(data), &client)
	if err != nil {
		return nil, err
	}

	return &client, nil
}

func (worker *ClientWorker) IsQuiet() bool {
	return worker.state != Running
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientWorker) Signal(newstate WorkerState) {
	if worker.state == Running {
		worker.state = newstate
		return
	}

	// only allow running -> quiet -> terminate
	// can't go from quiet -> running, terminate -> quiet, etc.
	if worker.state == Quiet && newstate == Terminate {
		worker.state = newstate
		return
	}

	if worker.state == Terminate {
		return
	}
}

func (worker *ClientWorker) IsConsumer() bool {
	return worker.Wid != ""
}

func (worker *ClientWorker) BusyCount() int {
	workingMutex.Lock()
	defer workingMutex.Unlock()

	count := 0
	for _, res := range workingMap {
		if res.Wid == worker.Wid {
			count += 1
		}
	}
	return count
}

type beatReaper struct {
	s     *Server
	count int
}

func (r *beatReaper) Name() string {
	return "Workers"
}

func (r *beatReaper) Stats() map[string]interface{} {
	r.s.hbmu.RLock()
	defer r.s.hbmu.RUnlock()
	return map[string]interface{}{
		"size":   len(r.s.heartbeats),
		"reaped": r.count,
	}
}

/*
 * Removes any heartbeat records over 1 minute old.
 */
func (r *beatReaper) Execute() error {
	r.count += reapHeartbeats(r.s.heartbeats, &r.s.hbmu)
	return nil
}

func reapHeartbeats(heartbeats map[string]*ClientWorker, mu *sync.RWMutex) int {
	toDelete := []string{}

	mu.RLock()
	for k, worker := range heartbeats {
		if worker.lastHeartbeat.Before(time.Now().Add(-1 * time.Minute)) {
			toDelete = append(toDelete, k)
		}
	}
	mu.RUnlock()

	count := len(toDelete)
	if count > 0 {
		mu.Lock()
		for _, k := range toDelete {
			delete(heartbeats, k)
		}
		mu.Unlock()

		util.Debugf("Reaped %d worker heartbeats", count)
	}
	return count
}

func updateHeartbeat(client *ClientWorker, heartbeats map[string]*ClientWorker, mu *sync.RWMutex) {
	mu.RLock()
	val, ok := heartbeats[client.Wid]
	mu.RUnlock()

	if ok {
		mu.Lock()
		val.lastHeartbeat = time.Now()
		mu.Unlock()
	} else {
		client.StartedAt = time.Now()
		client.lastHeartbeat = time.Now()
		mu.Lock()
		heartbeats[client.Wid] = client
		mu.Unlock()
		val = client
	}
}
