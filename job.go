package worq

import (
	"container/list"
	"encoding/json"
	"errors"
	"time"

	"github.com/mperham/worq/util"
)

var (
	DefaultTimeout = 600
)

type Failure struct {
	RetryCount   int      `json:"retry_count"`
	FailedAt     string   `json:"failed_at"`
	ErrorMessage string   `json:"message"`
	ErrorType    string   `json:"errtype"`
	Backtrace    []string `json:"backtrace"`
}

type Job struct {
	// required
	Jid   string        `json:"jid"`
	Queue string        `json:"queue"`
	Type  string        `json:"jobtype"`
	Args  []interface{} `json:"args"`

	// optional
	Gid        string                 `json:"gid"`
	CreatedAt  string                 `json:"created_at"`
	EnqueuedAt string                 `json:"enqueued_at"`
	ReserveFor int                    `json:"reserve_for"`
	Retry      int                    `json:"retry"`
	Backtrace  int                    `json:"backtrace"`
	Failure    *Failure               `json:"failure"`
	Custom     map[string]interface{} `json:"custom"`
}

func ParseJob(buf []byte) (*Job, error) {
	var job Job

	err := json.Unmarshal(buf, &job)
	if err != nil {
		return nil, err
	}

	if job.CreatedAt == "" {
		job.CreatedAt = util.Nows()
	}
	return &job, nil
}

type Reservation struct {
	Job    *Job      `json:"job"`
	Since  time.Time `json:"reserved_at"`
	Expiry time.Time `json:"expires_at"`
	Who    string    `json:"worker"`
}

var (
	workingSet = list.New()
)

func Acknowledge(jid string) error {
	for e := workingSet.Front(); e != nil; e = e.Next() {
		res := e.Value.(*Reservation)
		if res.Job.Jid == jid {
			workingSet.Remove(e)
			return nil
		}
	}
	return errors.New("Job not found in working set: " + jid)
}

func ReapWorkingSet() int {
	count := 0
	now := time.Now()
	for e := workingSet.Front(); e != nil; e = e.Next() {
		res := e.Value.(*Reservation)
		if res.Expiry.Before(now) {
			workingSet.Remove(e)
			_ = LookupQueue(res.Job.Queue).Push(res.Job)
			count = count + 1
		}
	}
	return count
}

func Reserve(identity string, job *Job) error {
	now := time.Now()
	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	var res = &Reservation{
		Job:    job,
		Since:  now,
		Expiry: now.Add(time.Duration(timeout) * time.Second),
		Who:    identity,
	}

	workingSet.PushBack(res)
	return nil
}
