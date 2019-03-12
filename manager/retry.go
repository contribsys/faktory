package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type FailPayload struct {
	Jid          string   `json:"jid"`
	ErrorMessage string   `json:"message"`
	ErrorType    string   `json:"errtype"`
	Backtrace    []string `json:"backtrace"`
}

func (m *manager) Fail(failure *FailPayload) error {
	if failure == nil {
		return fmt.Errorf("No failure")
	}

	jid := failure.Jid
	if jid == "" {
		return fmt.Errorf("Missing JID")
	}

	cleanse(failure)

	return m.processFailure(jid, failure)
}

func cleanse(failure *FailPayload) {
	failure.ErrorType = strings.TrimSpace(failure.ErrorType)
	failure.ErrorMessage = strings.TrimSpace(failure.ErrorMessage)

	if failure.ErrorType != "" {
		if len(failure.ErrorType) > 100 {
			failure.ErrorType = failure.ErrorType[0:100]
		}
	} else {
		failure.ErrorType = "unknown"
	}

	if failure.ErrorMessage != "" {
		if len(failure.ErrorMessage) > 1000 {
			failure.ErrorMessage = failure.ErrorMessage[0:1000]
		}
	} else {
		failure.ErrorMessage = "unknown"
	}

	if failure.Backtrace == nil {
		failure.Backtrace = []string{}
	}
	if len(failure.Backtrace) > 50 {
		failure.Backtrace = failure.Backtrace[0:50]
	}
}

func (m *manager) clearReservation(jid string) *Reservation {
	m.workingMutex.Lock()
	res, ok := m.workingMap[jid]
	if !ok {
		m.workingMutex.Unlock()
		return nil
	}

	delete(m.workingMap, jid)
	m.workingMutex.Unlock()
	return res
}

func (m *manager) processFailure(jid string, failure *FailPayload) error {
	res := m.clearReservation(jid)
	if res == nil {
		return fmt.Errorf("Job not found %s", jid)
	}

	// when expiring overdue jobs in the working set, we remove in
	// bulk so this job is no longer in the working set already.
	if failure != JobReservationExpired {
		ok, err := m.store.Working().RemoveElement(res.Expiry, jid)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	m.store.Failure()

	job := res.Job
	if job.Retry == 0 {
		// no retry, no death, completely ephemeral, goodbye
		return nil
	}

	if job.Failure != nil {
		job.Failure.RetryCount++
		job.Failure.ErrorMessage = failure.ErrorMessage
		job.Failure.ErrorType = failure.ErrorType
		job.Failure.Backtrace = failure.Backtrace
	} else {
		job.Failure = &client.Failure{
			RetryCount:   0,
			FailedAt:     util.Nows(),
			ErrorMessage: failure.ErrorMessage,
			ErrorType:    failure.ErrorType,
			Backtrace:    failure.Backtrace,
		}
	}

	return callMiddleware(m.failChain, Ctx{context.Background(), job, m, res}, func() error {
		if job.Failure.RetryCount < job.Retry {
			return retryLater(m.store, job)
		}
		return sendToMorgue(m.store, job)
	})
}

func retryLater(store storage.Store, job *client.Job) error {
	when := util.Thens(nextRetry(job))
	job.Failure.NextAt = when
	bytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return store.Retries().AddElement(when, job.Jid, bytes)
}

func sendToMorgue(store storage.Store, job *client.Job) error {
	bytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	expiry := util.Thens(time.Now().Add(DeadTTL))
	return store.Dead().AddElement(expiry, job.Jid, bytes)
}

func nextRetry(job *client.Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1))
	return time.Now().Add(time.Duration(secs) * time.Second)
}
