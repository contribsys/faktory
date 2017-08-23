package server

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/util"
)

var (
	DefaultTimeout = 600
)

func (s *Server) Acknowledge(jid string) (*faktory.Job, error) {
	workingMutex.Lock()
	res, ok := workingMap[jid]
	if !ok {
		workingMutex.Unlock()
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

	delete(workingMap, jid)
	workingMutex.Unlock()

	err := s.store.Working().RemoveElement(util.Thens(res.texpiry), jid)
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

type Reservation struct {
	Job     *faktory.Job `json:"job"`
	Since   string       `json:"reserved_at"`
	Expiry  string       `json:"expires_at"`
	Wid     string       `json:"wid"`
	tsince  time.Time
	texpiry time.Time
}

var (
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

func (s *Server) Reserve(wid string, job *faktory.Job) error {
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
		Wid:     wid,
		tsince:  now,
		texpiry: exp,
	}

	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	err = s.store.Working().AddElement(util.Thens(res.texpiry), job.Jid, data)
	if err != nil {
		return err
	}

	workingMutex.Lock()
	workingMap[job.Jid] = res
	workingMutex.Unlock()
	return nil
}
