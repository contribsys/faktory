package server

import (
	"time"

	"github.com/mperham/faktory/util"
)

type ClientWorker struct {
	Hostname  string   `json:"hostname"`
	Wid       string   `json:"wid"`
	Pid       int      `json:"pid"`
	Labels    []string `json:"labels"`
	Password  string   `json:"password"`
	StartedAt time.Time

	lastHeartbeat time.Time
	signal        string
	state         string
}

func (worker *ClientWorker) Quiet() bool {
	return worker.state == "quiet"
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientWorker) Signal(sig string) {
	worker.signal = sig
	worker.state = sig
}

func (worker *ClientWorker) Busy() int {
	count := 0
	workingMutex.Lock()
	for _, res := range workingMap {
		if res.Wid == worker.Wid {
			count += 1
		}
	}
	workingMutex.Unlock()
	return count
}

/*
 * Removes any heartbeat records over 1 minute old.
 */
func (s *Server) reapHeartbeats() error {
	toDelete := []string{}

	for k, worker := range s.heartbeats {
		if worker.lastHeartbeat.Before(time.Now().Add(-1 * time.Minute)) {
			toDelete = append(toDelete, k)
		}
	}

	if len(toDelete) > 0 {
		for _, k := range toDelete {
			delete(s.heartbeats, k)
		}
		util.Debugf("Reaped %d worker heartbeats", len(toDelete))
	}
	return nil
}
