package manager

import (
	"encoding/json"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

func (m *manager) Purge() (int64, error) {
	// TODO We need to purge the dead set if it collects more
	// than N elements.  The dead set shouldn't be able to collect
	// millions or billions of jobs.  Sidekiq uses a default max size
	// of 10,000 jobs.
	dead, err := m.store.Dead().RemoveBefore(util.Nows())
	if err != nil {
		return 0, err
	}
	return int64(len(dead)), nil
}

func (m *manager) EnqueueScheduledJobs() (int64, error) {
	return m.schedule(m.store.Scheduled())
}

func (m *manager) RetryJobs() (int64, error) {
	return m.schedule(m.store.Retries())
}

func (m *manager) schedule(set storage.SortedSet) (int64, error) {
	elms, err := set.RemoveBefore(util.Nows())
	if err != nil {
		return 0, err
	}

	count := int64(0)
	for _, elm := range elms {
		var job client.Job
		err := json.Unmarshal(elm, &job)
		if err != nil {
			util.Error("Unable to unmarshal json", err)
			continue
		}

		err = m.enqueue(&job)
		if err != nil {
			util.Warnf("Error pushing job to '%s': %s", job.Queue, err.Error())
			continue
		}

		count++
	}

	return count, nil
}
