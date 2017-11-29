package manager

import (
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
)

var (
	JobReservationExpired = &FailPayload{
		ErrorType:    "ReservationExpired",
		ErrorMessage: "Faktory job reservation expired",
	}
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

func (m *manager) BusyCount(wid string) int {
	m.workingMutex.RLock()

	count := 0
	for _, res := range m.workingMap {
		if res.Wid == wid {
			count++
		}
	}
	m.workingMutex.RUnlock()
	return count
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
	res := m.clearReservation(jid)
	if res == nil {
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

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

func (m *manager) ReapExpiredJobs(timestamp string) (int, error) {
	elms, err := m.store.Working().RemoveBefore(timestamp)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, elm := range elms {
		var res Reservation
		err := json.Unmarshal(elm, &res)
		if err != nil {
			util.Error("Unable to read reservation", err)
			continue
		}

		job := res.Job
		err = m.processFailure(job.Jid, JobReservationExpired)
		if err != nil {
			util.Error("Unable to retry reservation", err)
			continue
		}
		count++
	}

	return count, nil
}
