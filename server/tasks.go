package server

import (
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

type reservationReaper struct {
	m     manager.Manager
	count int
}

func (r *reservationReaper) Name() string {
	return "Busy"
}

func (r *reservationReaper) Execute() error {
	count, err := r.m.ReapLongRunningJobs(util.Nows())
	if err != nil {
		return err
	}

	r.count += count
	return nil
}

func (r *reservationReaper) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":   r.m.WorkingCount(),
		"reaped": r.count,
	}
}
