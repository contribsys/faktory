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
			err = m.Push(job)
			if err != nil {
				util.Error("Unable to push job", err)
				continue
			}

			count += 1
		}
	}

	return count, nil
}
