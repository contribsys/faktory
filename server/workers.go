package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
)

//
// This represents a single client process.  It may have many network
// connections open to Faktory.
//
// A client can be a producer AND/OR consumer of jobs.  Typically a process will
// either only produce jobs (like a webapp pushing jobs) or produce/consume jobs
// (like a faktory worker process where a job can create other jobs while
// executing).
//
// Each Faktory worker process should send a BEAT command every 15 seconds.
// Only consumers should send a BEAT.  If Faktory does not receive a BEAT from a
// worker process within 60 seconds, it expires and is removed from the Busy
// page.
//
// From Faktory's POV, the worker can BEAT again and resume normal operations,
// e.g. due to a network partition.  If a process dies, it will be removed
// after 1 minute and its jobs recovered after the job reservation timeout has
// passed (typically 30 minutes).
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
// A worker process should never stop sending BEAT.  Even after "quiet" or
// "terminate", the BEAT should continue, only stopping due to process exit().
// Workers should never move backward in state - you cannot "unquiet" a worker,
// it must be restarted.
//
// Workers will typically also respond to standard Unix signals.
// faktory_worker_ruby uses TSTP ("Threads SToP") as the quiet signal and TERM as the terminate signal.
//
type ClientData struct {
	Hostname     string   `json:"hostname"`
	Wid          string   `json:"wid"`
	Pid          int      `json:"pid"`
	Labels       []string `json:"labels"`
	PasswordHash string   `json:"pwdhash"`
	Version      uint8    `json:"v"`
	StartedAt    time.Time

	// this only applies to clients that are workers and
	// are sending BEAT
	lastHeartbeat time.Time
	state         WorkerState
}

type WorkerState int

const (
	Running WorkerState = iota
	Quiet
	Terminate
)

func stateString(state WorkerState) string {
	switch state {
	case Quiet:
		return "quiet"
	case Terminate:
		return "terminate"
	default:
		return ""
	}
}

func clientDataFromHello(data string) (*ClientData, error) {
	var client ClientData
	err := json.Unmarshal([]byte(data), &client)
	if err != nil {
		return nil, err
	}

	return &client, nil
}

func (worker *ClientData) IsQuiet() bool {
	return worker.state != Running
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientData) Signal(newstate WorkerState) {
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

func (worker *ClientData) IsConsumer() bool {
	return worker.Wid != ""
}

type workers struct {
	heartbeats map[string]*ClientData
	mu         sync.RWMutex
}

func newWorkers() *workers {
	return &workers{
		heartbeats: make(map[string]*ClientData, 12),
	}
}

func (w *workers) Count() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.heartbeats)
}

func (w *workers) heartbeat(client *ClientData, register bool) (*ClientData, bool) {
	w.mu.RLock()
	entry, ok := w.heartbeats[client.Wid]
	w.mu.RUnlock()

	if ok {
		w.mu.Lock()
		entry.lastHeartbeat = time.Now()
		w.mu.Unlock()
	} else if register {
		client.StartedAt = time.Now()
		client.lastHeartbeat = time.Now()
		w.mu.Lock()
		w.heartbeats[client.Wid] = client
		w.mu.Unlock()
		entry = client
		ok = true
	}

	return entry, ok
}

func (w *workers) reapHeartbeats(t time.Time) int {
	toDelete := []string{}

	w.mu.RLock()
	for k, worker := range w.heartbeats {
		if worker.lastHeartbeat.Before(t) {
			toDelete = append(toDelete, k)
		}
	}
	w.mu.RUnlock()

	count := len(toDelete)
	if count > 0 {
		w.mu.Lock()
		for _, k := range toDelete {
			delete(w.heartbeats, k)
		}
		w.mu.Unlock()

		util.Debugf("Reaped %d worker heartbeats", count)
	}
	return count
}
