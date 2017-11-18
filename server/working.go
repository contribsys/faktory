package server

import (
	"encoding/json"

	"github.com/contribsys/faktory/util"
)

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
	if addedCount > 0 {
		util.Debugf("Bootstrapped working set, loaded %d", addedCount)
	}
	return err
}

type reservationReaper struct {
	s     *Server
	count int
}

func (r *reservationReaper) Name() string {
	return "Busy"
}

func (r *reservationReaper) Execute() error {
	count, err := r.s.manager.ReapLongRunningJobs(util.Nows())
	if err != nil {
		return err
	}

	r.count += count
	return nil
}

func (r *reservationReaper) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":   r.s.manager.WorkingCount(),
		"reaped": r.count,
	}
}
