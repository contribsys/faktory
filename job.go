package worq

import (
	"encoding/json"
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
	// Hold the working set in memory so we don't need to burn CPU
	// marshalling between Bolt and memory when doing 1000s of jobs/sec.
	// When client ack's JID, we can lookup reservation
	// and remove Bolt entry quickly.
	//
	// TODO Need to hydrate this map into memory when starting up
	// or a crash can leak reservations into the persistent Working
	// set.
	workingMap = map[string]*Reservation{}
)

func workingSize() int {
	return len(workingMap)
}
