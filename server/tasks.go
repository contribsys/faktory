package server

import (
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/storage"
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
	count, err := r.m.ReapExpiredJobs(util.Nows())
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

/*
 * Removes any heartbeat records over 1 minute old.
 */
type beatReaper struct {
	w     *workers
	count int
}

func (r *beatReaper) Name() string {
	return "Workers"
}

func (r *beatReaper) Execute() error {
	r.count += r.w.reapHeartbeats(time.Now().Add(-1 * time.Minute))
	return nil
}

func (r *beatReaper) Stats() map[string]interface{} {
	return map[string]interface{}{
		"size":   r.w.Count(),
		"reaped": r.count,
	}
}

type cacheReset struct {
	s     storage.Store
	count int
}

func (r *cacheReset) Name() string {
	return "Storage Cache"
}

func (r *cacheReset) Execute() error {
	r.count += r.s.Retries().Reset()
	r.count += r.s.Scheduled().Reset()
	r.count += r.s.Working().Reset()
	r.count += r.s.Dead().Reset()
	r.s.Compact()
	return nil
}

func (r *cacheReset) Stats() map[string]interface{} {
	return map[string]interface{}{
		"resetCount": r.count,
	}
}
