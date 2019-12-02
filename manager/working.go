package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
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
	lease   Lease
}

func (res *Reservation) ReservedAt() time.Time {
	return res.tsince
}

func (res *Reservation) ExpiresAt() time.Time {
	return res.texpiry
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
	err := m.store.Working().Each(func(idx int, entry storage.SortedEntry) error {
		var res Reservation
		err := json.Unmarshal(entry.Value(), &res)
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

func (m *manager) reserve(wid string, lease Lease) error {
	now := time.Now()
	job, _ := lease.Job()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	if timeout < 60 {
		util.Debugf("Timeout too short %d, 60 seconds minimum", timeout)
		timeout = 60
	}

	if timeout > 86400 {
		util.Debugf("Timeout too long %d, one day maximum", timeout)
		timeout = 86400
	}

	exp := now.Add(time.Duration(timeout) * time.Second)
	var res = &Reservation{
		lease:   lease,
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

func (m *manager) Acknowledge(jid string) (*client.Job, error) {
	res := m.clearReservation(jid)
	if res == nil {
		util.Infof("No such job to acknowledge %s", jid)
		return nil, nil
	}

	// doesn't matter, might not have acknowledged in time
	_, err := m.store.Working().RemoveElement(res.Expiry, jid)
	if err != nil {
		return nil, err
	}

	// Lease is in-memory only
	// A reservation can have a nil Lease if we restarted
	if res.lease != nil {
		res.lease.Release()
	}

	if res.Job != nil {
		m.store.Success()
		err = callMiddleware(m.ackChain, Ctx{context.Background(), res.Job, m, res}, func() error {
			return nil
		})
	}

	return res.Job, err
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
