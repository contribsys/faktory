package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/util"
)

var (
	DefaultTimeout = 1800
	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Rocks and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Rocks entry quickly.
	//
	// TODO Need to hydrate this map into memory when starting up
	// or a crash can leak reservations into the persistent Working
	// set.
	workingMap   = map[string]*Reservation{}
	workingMutex = &sync.Mutex{}
)

type Reservation struct {
	Job     *faktory.Job `json:"job"`
	Since   string       `json:"reserved_at"`
	Expiry  string       `json:"expires_at"`
	Wid     string       `json:"wid"`
	tsince  time.Time
	texpiry time.Time
}

type TimedSet interface {
	AddElement(string, string, []byte) error
	RemoveElement(string, string) error
}

func acknowledge(jid string, set TimedSet) (*faktory.Job, error) {
	workingMutex.Lock()
	res, ok := workingMap[jid]
	if !ok {
		workingMutex.Unlock()
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

	delete(workingMap, jid)
	workingMutex.Unlock()

	err := set.RemoveElement(res.Expiry, jid)
	return res.Job, err
}

func (s *Server) ReapWorkingSet() (int, error) {
	count := 0

	jobs, err := s.store.Working().RemoveBefore(util.Nows())
	if err != nil {
		return 0, err
	}

	for _, data := range jobs {
		var job faktory.Job
		err := json.Unmarshal(data, &job)
		if err != nil {
			util.Error("Unable to unmarshal job", err, nil)
			continue
		}

		q, err := s.store.GetQueue(job.Queue)
		if err != nil {
			util.Error("Unable to retrieve queue", err, nil)
			continue
		}

		workingMutex.Lock()
		_, ok := workingMap[job.Jid]
		if ok {
			delete(workingMap, job.Jid)
		}
		workingMutex.Unlock()

		if ok {
			err = q.Push(data)
			if err != nil {
				util.Error("Unable to push job", err, nil)
				continue
			}

			count += 1
		}
	}

	return count, nil
}

func reserve(wid string, job *faktory.Job, set TimedSet) error {
	now := time.Now()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout < 600 {
		timeout = DefaultTimeout
		util.Warnf("Timeout too short %d, 600 seconds minimum", timeout)
	}

	if timeout > 86400 {
		timeout = DefaultTimeout
		util.Warnf("Timeout too long %d, one day maximum", timeout)
	}

	exp := now.Add(time.Duration(timeout) * time.Second)
	var res = &Reservation{
		Job:     job,
		Since:   util.Thens(now),
		Expiry:  util.Thens(exp),
		Wid:     wid,
		tsince:  now,
		texpiry: exp,
	}

	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	err = set.AddElement(res.Expiry, job.Jid, data)
	if err != nil {
		return err
	}

	workingMutex.Lock()
	workingMap[job.Jid] = res
	workingMutex.Unlock()

	return nil
}
