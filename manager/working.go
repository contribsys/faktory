package manager

import (
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
)

type Reservation struct {
	Job     *client.Job `json:"job"`
	Since   string      `json:"reserved_at"`
	Expiry  string      `json:"expires_at"`
	Wid     string      `json:"wid"`
	tsince  time.Time
	texpiry time.Time
}

func (m *manager) WorkingCount() int {
	m.workingMutex.RLock()
	defer m.workingMutex.RUnlock()
	return len(m.workingMap)
}

/*
 * When we restart the server, we need to load the
 * current set of Reservations back into memory so any
 * outstanding jobs can be Acknowledged successfully.
 *
 * The alternative is that a server restart would re-execute
 * all outstanding jobs, something to be avoided when possible.
 */
func (m *manager) loadWorkingSet() error {
	m.workingMutex.Lock()
	defer m.workingMutex.Unlock()

	addedCount := 0
	err := m.store.Working().Each(func(_ int, _ []byte, data []byte) error {
		var res Reservation
		err := json.Unmarshal(data, &res)
		if err != nil {
			return err
		}
		m.workingMap[res.Job.Jid] = &res
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

func (m *manager) reserve(wid string, job *client.Job) error {
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

	err = m.store.Working().AddElement(res.Expiry, job.Jid, data)
	if err != nil {
		return err
	}

	m.workingMutex.Lock()
	m.workingMap[job.Jid] = res
	m.workingMutex.Unlock()

	return nil
}

func (m *manager) ack(jid string) (*client.Job, error) {
	m.workingMutex.Lock()
	res, ok := m.workingMap[jid]
	if !ok {
		m.workingMutex.Unlock()
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

	delete(m.workingMap, jid)
	m.workingMutex.Unlock()

	err := m.store.Working().RemoveElement(res.Expiry, jid)
	return res.Job, err
}

func (m *manager) Acknowledge(jid string) (*client.Job, error) {
	job, err := m.ack(jid)
	if err != nil {
		return nil, err
	}

	if job != nil {
		m.store.Success()
	}
	return job, nil
}

func (m *manager) ReapLongRunningJobs(timestamp string) (int, error) {
	reservations, err := m.store.Working().RemoveBefore(timestamp)
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

		m.workingMutex.Lock()
		_, ok := m.workingMap[job.Jid]
		if ok {
			delete(m.workingMap, job.Jid)
		}
		m.workingMutex.Unlock()

		if ok {
			err = m.enqueue(job)
			if err != nil {
				util.Error("Unable to push job", err)
				continue
			}

			count++
		}
	}

	return count, nil
}
