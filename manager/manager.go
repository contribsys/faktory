package manager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

const DefaultTimeout = 1800

type Manager interface {
	Push(job *client.Job) error
}

func NewManager(s storage.Store) Manager {
	return &manager{store: s}
}

type manager struct {
	store storage.Store
}

func (m *manager) Push(job *client.Job) error {
	if job.Jid == "" || len(job.Jid) < 8 {
		return fmt.Errorf("All jobs must have a reasonable jid parameter")
	}
	if job.Type == "" {
		return fmt.Errorf("All jobs must have a jobtype parameter")
	}
	if job.Args == nil {
		return fmt.Errorf("All jobs must have an args parameter")
	}

	// Priority can never be negative because of signedness
	if job.Priority > 9 || job.Priority == 0 {
		job.Priority = 5
	}

	if job.At != "" {
		t, err := util.ParseTime(job.At)
		if err != nil {
			return fmt.Errorf("Invalid timestamp for 'at': '%s'", job.At)
		}

		if t.After(time.Now()) {
			data, err := json.Marshal(job)
			if err != nil {
				return err
			}

			// scheduler for later
			err = m.store.Scheduled().AddElement(job.At, job.Jid, data)
			if err != nil {
				return err
			}
			return nil
		}
	}

	// enqueue immediately
	q, err := m.store.GetQueue(job.Queue)
	if err != nil {
		return err
	}

	job.EnqueuedAt = util.Nows()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = q.Push(job.Priority, data)
	if err != nil {
		return err
	}

	return nil
}
