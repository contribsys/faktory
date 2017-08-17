package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/util"
)

var (
	// about one month
	maxRetryDelay = 720 * time.Hour
)

func fail(c *Connection, s *Server, cmd string) {
	raw := cmd[5:]
	elms := strings.SplitN(raw, " ", 2)
	jid := elms[0]
	errtype := "unknown"
	msg := "unknown"
	var backtrace []string

	var failure faktory.Failure
	if len(elms) == 2 {
		hash := elms[1]
		err := json.Unmarshal([]byte(hash), &failure)
		if err != nil {
			c.Error(cmd, err)
			return
		}
		if failure.ErrorType != "" {
			errtype = failure.ErrorType
		}
		if failure.ErrorMessage != "" {
			msg = failure.ErrorMessage
		}
		backtrace = failure.Backtrace
	}

	err := s.Fail(jid, msg, errtype, backtrace)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	util.Debugf("%s Failure %v", jid, failure)

	c.Ok()
}

func (s *Server) Fail(jid, msg, errtype string, backtrace []string) error {
	job, err := s.Acknowledge(jid)
	if err != nil {
		return err
	}
	if job == nil {
		// job has already been ack'd?
		return fmt.Errorf("Cannot fail %s, not found in working set", jid)
	}

	if job.Failure != nil {
		job.Failure.RetryCount += 1
		job.Failure.ErrorMessage = msg
		job.Failure.ErrorType = errtype
		job.Failure.Backtrace = backtrace
	} else {
		job.Failure = &faktory.Failure{
			RetryCount:   0,
			FailedAt:     util.Nows(),
			ErrorMessage: msg,
			ErrorType:    errtype,
			Backtrace:    backtrace,
		}
	}

	when := util.Thens(nextRetry(job))
	job.Failure.NextAt = when
	bytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = s.store.Retries().AddElement(when, job.Jid, bytes)
	atomic.AddInt64(&s.Failures, 1)
	return nil
}

func nextRetry(job *faktory.Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1))
	return time.Now().Add(time.Duration(secs) * time.Second)
}
