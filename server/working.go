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

/*
 * When we restart the server, we need to load the
 * current set of Reservations back into memory so any
 * outstanding jobs can be Acknowledged successfully.
 *
 * The alternative is that a server restart would re-execute
 * all outstanding jobs, something to be avoided when possible.
 */
func (s *Server) loadWorkingSet() error {
	workingMutex.Lock()
	defer workingMutex.Unlock()

	addedCount := 0
	err := s.store.Working().Each(func(_ int, _ []byte, data []byte) error {
		var res Reservation
		err := json.Unmarshal(data, &res)
		if err != nil {
			return err
		}
		workingMap[res.Job.Jid] = &res
		addedCount += 1
		return nil
	})
	if err != nil {
		return err
	}
	reapedCount, err := s.reapWorkingSet()
	if addedCount > 0 || reapedCount > 0 {
		util.Debugf("Bootstrap working set, loaded %d, reaped %d", addedCount, reapedCount)
	}
	return err
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

func (s *Server) reapWorkingSet() (int, error) {
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
