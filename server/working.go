package server

import (
	"encoding/json"

	"github.com/contribsys/faktory/util"
)

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
