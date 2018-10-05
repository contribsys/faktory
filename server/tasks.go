package server

import (
	"sync/atomic"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

type reservationReaper struct {
	m     manager.Manager
	count int64
}

func (r *reservationReaper) Name() string {
	return "Busy"
}

func (r *reservationReaper) Execute() error {
	count, err := r.m.ReapExpiredJobs(util.Nows())
	if err != nil {
		return err
	}

	atomic.AddInt64(&r.count, int64(count))
	return nil
}

func (r *reservationReaper) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":   r.m.WorkingCount(),
		"reaped": atomic.LoadInt64(&r.count),
	}
}

/*
 * Removes any heartbeat records over 1 minute old.
 */
type beatReaper struct {
	w     *workers
	count int64
}

func (r *beatReaper) Name() string {
	return "Workers"
}

func (r *beatReaper) Execute() error {
	count := r.w.reapHeartbeats(time.Now().Add(-1 * time.Minute))
	atomic.AddInt64(&r.count, int64(count))
	return nil
}

func (r *beatReaper) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":   r.w.Count(),
		"reaped": atomic.LoadInt64(&r.count),
	}
}
