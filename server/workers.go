package server

import "time"

type ClientWorker struct {
	Hostname  string   `json:"hostname"`
	Wid       string   `json:"wid"`
	Pid       int      `json:"pid"`
	Labels    []string `json:"labels"`
	Password  string   `json:"password"`
	StartedAt time.Time

	lastHeartbeat time.Time
	signal        string
}

func (worker *ClientWorker) Quiet() bool {
	for _, lbl := range worker.Labels {
		if lbl == "quiet" {
			return true
		}
	}
	return false
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientWorker) Signal(sig string) {
	worker.signal = sig
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

	for _, k := range toDelete {
		delete(s.heartbeats, k)
	}
	return nil
}
