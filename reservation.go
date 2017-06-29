package worq

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mperham/worq/util"
)

func (s *Server) Acknowledge(jid string) (*Job, error) {
	workingMutex.Lock()
	defer workingMutex.Unlock()

	res, ok := workingMap[jid]
	if !ok {
		return nil, fmt.Errorf("JID %s not found", jid)
	}
	delete(workingMap, jid)
	return res.Job, nil
}

func (s *Server) ReapWorkingSet() (int, error) {
	now := time.Now()
	count := 0

	workingMutex.Lock()
	defer workingMutex.Unlock()

	for jid, res := range workingMap {
		if res.texpiry.Before(now) {
			delete(workingMap, jid)
			count += 1
		}
	}

	return count, nil
}

type Reservation struct {
	Job     *Job   `json:"job"`
	Since   string `json:"reserved_at"`
	Expiry  string `json:"expires_at"`
	Who     string `json:"worker"`
	tsince  time.Time
	texpiry time.Time
}

var (
	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Bolt and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Bolt entry quickly.
	//
	// TODO Need to hydrate this map into memory when starting up
	// or a crash can leak reservations into the persistent Working
	// set.
	workingMap   = map[string]*Reservation{}
	workingMutex = &sync.Mutex{}
)

func workingSize() int {
	return len(workingMap)
}

func Reserve(identity string, job *Job) error {
	now := time.Now()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	exp := now.Add(time.Duration(timeout) * time.Second)
	var res = &Reservation{
		Job:     job,
		Since:   util.Thens(now),
		Expiry:  util.Thens(exp),
		Who:     identity,
		tsince:  now,
		texpiry: exp,
	}

	_, err := json.Marshal(res)
	if err != nil {
		return err
	}
	workingMutex.Lock()
	workingMap[job.Jid] = res
	workingMutex.Unlock()
	return nil
}
