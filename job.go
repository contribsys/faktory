package worq

import (
	"bytes"
	"encoding/gob"
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
	Job   *Job      `json:"job"`
	Since time.Time `json:"reserved_at"`
	Who   string    `json:"worker"`
}

func Reserve(s *Server, conn *Connection, job *Job) error {
	var res = Reservation{
		Job:   job,
		Since: time.Now(),
		Who:   conn.Identity(),
	}

	timeout := job.ReserveFor
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(res)
	if err != nil {
		return err
	}

	return s.store.Working().AddElement(time.Now().Add(time.Duration(timeout)*time.Second), job.Jid, buf.Bytes())
}
