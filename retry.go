package worq

import (
	"encoding/json"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/mperham/worq/util"
)

type JobFailure struct {
	Jid          string   `json:"jid"`
	RetryAt      string   `json:"retry_at"`
	ErrorMessage string   `json:"message"`
	ErrorType    string   `json:"errtype"`
	Backtrace    []string `json:"backtrace"`
}

var (
	// about one month
	maxRetryDelay = 720 * time.Hour
)

func fail(c *Connection, s *Server, cmd string) {
	raw := cmd[5:]
	var failure JobFailure
	err := json.Unmarshal([]byte(raw), &failure)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	job, err := s.Acknowledge(failure.Jid)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	if job.Failure != nil {
		job.Failure.RetryCount += 1
		job.Failure.ErrorMessage = failure.ErrorMessage
		job.Failure.ErrorType = failure.ErrorType
		job.Failure.Backtrace = failure.Backtrace
	} else {
		job.Failure = &Failure{
			RetryCount:   0,
			FailedAt:     util.Nows(),
			ErrorMessage: failure.ErrorMessage,
			ErrorType:    failure.ErrorType,
			Backtrace:    failure.Backtrace,
		}
	}

	when := nextRetry(failure.RetryAt, job)
	bytes, err := json.Marshal(job)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	util.Info("Adding retry", job.Jid, when)
	s.store.Retries().AddElement(util.Thens(when), job.Jid, bytes)
	atomic.AddInt64(&s.Failures, 1)
	c.Ok()
}

func nextRetry(override string, job *Job) time.Time {
	if override != "" {
		tm, err := time.Parse(time.RFC3339, override)
		if err != nil {
			util.Warn("Invalid retry_at: %s", override)
			return defaultRetry(job)
		}
		if tm.Before(time.Now()) || tm.After(time.Now().Add(maxRetryDelay)) {
			// retry time out of range
			util.Warn("Invalid retry_at time: %s", tm)
			return defaultRetry(job)
		}
		return tm
	}
	return defaultRetry(job)
}

func defaultRetry(job *Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1))
	return time.Now().Add(time.Duration(secs) * time.Second)
}
