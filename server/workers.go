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
