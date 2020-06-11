package manager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

func (m *manager) Purge(when time.Time) (int64, error) {
	// TODO We need to purge the dead set if it collects more
	// than N elements.  The dead set shouldn't be able to collect
	// millions or billions of jobs.  Sidekiq uses a default max size
	// of 10,000 jobs.
	dead, err := m.store.Dead().RemoveBefore(util.Thens(when), 100, func([]byte) error {
		return nil
	})
	if err != nil {
		return 0, err
	}
	return dead, nil
}

func (m *manager) EnqueueScheduledJobs(when time.Time) (int64, error) {
	return m.schedule(when, m.store.Scheduled())
}

func (m *manager) RetryJobs(when time.Time) (int64, error) {
	return m.schedule(when, m.store.Retries())
}

func (m *manager) schedule(when time.Time, set storage.SortedSet) (int64, error) {
	total := int64(0)
	for {
		count, err := set.RemoveBefore(util.Thens(when), 100, func(data []byte) error {
			var job client.Job
			err := json.Unmarshal(data, &job)
			if err != nil {
				return err
			}

			err = m.enqueue(&job)
			if err != nil {
				return fmt.Errorf("Error pushing job to '%s': %w", job.Queue, err)
			}
			return nil
		})
		total += count
		if err != nil {
			return total, err
		}
		if count != 100 {
			break
		}
	}
	return total, nil
}
