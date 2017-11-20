package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

var (
	DefaultTimeout = 1800

	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Rocks and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Rocks entry quickly.
	workingMap   = map[string]*Reservation{}
	workingMutex = &sync.RWMutex{}
)

type Reservation struct {
	Job     *client.Job `json:"job"`
	Since   string      `json:"reserved_at"`
	Expiry  string      `json:"expires_at"`
	Wid     string      `json:"wid"`
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
		addedCount++
		return nil
	})
	if err != nil {
		return err
	}
	if addedCount > 0 {
		util.Debugf("Bootstrapped working set, loaded %d", addedCount)
	}
	return err
}

func acknowledge(jid string, set TimedSet) (*client.Job, error) {
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

type reservationReaper struct {
	s     *Server
	count int
}

func (r *reservationReaper) Name() string {
	return "Busy"
}

func (r *reservationReaper) Execute() error {
	count, err := reapLongRunningJobs(r.s.store, util.Nows())
	if err != nil {
		return err
	}

	r.count += count
	return nil
}

func reapLongRunningJobs(store storage.Store, timestamp string) (int, error) {
	reservations, err := store.Working().RemoveBefore(timestamp)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, data := range reservations {
		var res Reservation
		err := json.Unmarshal(data, &res)
		if err != nil {
			util.Error("Unable to unmarshal reservation", err)
			continue
		}

		job := res.Job
		q, err := store.GetQueue(job.Queue)
		if err != nil {
			util.Error("Unable to retrieve queue", err)
			continue
		}

		workingMutex.Lock()
		_, ok := workingMap[job.Jid]
		if ok {
			delete(workingMap, job.Jid)
		}
		workingMutex.Unlock()

		if ok {
			job.EnqueuedAt = util.Nows()
			data, err = json.Marshal(job)
			if err != nil {
				util.Error("Unable to serialize job", err)
				continue
			}
			err = q.Push(job.Priority, data)
			if err != nil {
				util.Error("Unable to push job", err)
				continue
			}

			count++
		}
	}

	return count, nil
}

func (r *reservationReaper) Stats() map[string]interface{} {
	workingMutex.RLock()
	defer workingMutex.RUnlock()
	return map[string]interface{}{
		"size":   len(workingMap),
		"reaped": r.count,
	}
}

func reserve(wid string, job *client.Job, set TimedSet) error {
	now := time.Now()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout < 60 {
		timeout = DefaultTimeout
		util.Warnf("Timeout too short %d, 60 seconds minimum", timeout)
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
