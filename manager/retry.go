package manager

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

// six months
var deadTTL = 180 * 24 * time.Hour

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

	errtype := "unknown"
	msg := "unknown"
	var backtrace []string

	if failure.ErrorType != "" {
		errtype = failure.ErrorType
		if len(errtype) > 100 {
			errtype = errtype[0:100]
		}
	}

	if failure.ErrorMessage != "" {
		msg = failure.ErrorMessage
		if len(msg) > 1000 {
			msg = msg[0:1000]
		}
	}

	backtrace = failure.Backtrace
	if len(backtrace) > 50 {
		backtrace = backtrace[0:50]
	}

	return m.processFailure(jid, msg, errtype, backtrace)
}

func (m *manager) processFailure(jid, msg, errtype string, backtrace []string) error {
	job, err := m.Acknowledge(jid)
	if err != nil {
		return err
	}
	if job == nil {
		// job has already been ack'd?
		return fmt.Errorf("Cannot fail %s, not found in working set", jid)
	}

	m.store.Failure()

	if job.Retry == 0 {
		// no retry, no death, completely ephemeral, goodbye
		return nil
	}

	if job.Failure != nil {
		job.Failure.RetryCount += 1
		job.Failure.ErrorMessage = msg
		job.Failure.ErrorType = errtype
		job.Failure.Backtrace = backtrace
	} else {
		job.Failure = &client.Failure{
			RetryCount:   0,
			FailedAt:     util.Nows(),
			ErrorMessage: msg,
			ErrorType:    errtype,
			Backtrace:    backtrace,
		}
	}

	if job.Failure.RetryCount < job.Retry {
		return retryLater(m.store, job)
	}
	return sendToMorgue(m.store, job)
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

	expiry := util.Thens(time.Now().Add(deadTTL))
	return store.Dead().AddElement(expiry, job.Jid, bytes)
}

func nextRetry(job *client.Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1))
	return time.Now().Add(time.Duration(secs) * time.Second)
}
