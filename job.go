package faktory

import (
	"encoding/json"

	"github.com/mperham/faktory/util"
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
