package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
)

type ClientWorker struct {
	Hostname     string   `json:"hostname"`
	Wid          string   `json:"wid"`
	Pid          int      `json:"pid"`
	Labels       []string `json:"labels"`
	Salt         string   `json:"salt"`
	PasswordHash string   `json:"pwdhash"`
	StartedAt    time.Time

	lastHeartbeat time.Time
	signal        string
	state         string
}

func clientWorkerFromAhoy(data string) (*ClientWorker, error) {
	var client ClientWorker
	err := json.Unmarshal([]byte(data), &client)
	if err != nil {
		return nil, err
	}

	if client.Wid == "" {
		return nil, fmt.Errorf("Invalid client Wid")
	}

	return &client, nil
}

func (worker *ClientWorker) IsQuiet() bool {
	return worker.state != ""
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientWorker) Signal(sig string) {
	worker.signal = sig
	worker.state = sig
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

/*
 * Removes any heartbeat records over 1 minute old.
 */
func reapHeartbeats(heartbeats map[string]*ClientWorker, mu *sync.Mutex) error {
	toDelete := []string{}

	for k, worker := range heartbeats {
		if worker.lastHeartbeat.Before(time.Now().Add(-1 * time.Minute)) {
			toDelete = append(toDelete, k)
		}
	}

	if len(toDelete) > 0 {
		mu.Lock()
		for _, k := range toDelete {
			delete(heartbeats, k)
		}
		mu.Unlock()
		util.Debugf("Reaped %d worker heartbeats", len(toDelete))
	}
	return nil
}

func updateHeartbeat(client *ClientWorker, heartbeats map[string]*ClientWorker, mu *sync.Mutex) {
	mu.Lock()
	val, ok := heartbeats[client.Wid]
	if ok {
		val.lastHeartbeat = time.Now()
	} else {
		client.StartedAt = time.Now()
		client.lastHeartbeat = time.Now()
		heartbeats[client.Wid] = client
		val = client
	}
	mu.Unlock()
	util.Debugf("%+v", val)
}
