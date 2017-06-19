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

type Job struct {
	Jid        string        `json:"jid"`
	Gid        string        `json:"gid"`
	Queue      string        `json:"queue"`
	EnqueuedAt float64       `json:"enqueued_at"`
	CreatedAt  float64       `json:"created_at"`
	Class      string        `json:"class"`
	Args       []interface{} `json:"args"`
	Timeout    int           `json:"reserve_for"`

	FailedAt     float64  `json:"failed_at"`
	RetryCount   int32    `json:"retry_count"`
	ErrorMessage string   `json:"error_message"`
	ErrorType    string   `json:"error_type"`
	Backtrace    []string `json:"backtrace"`
}

func ParseJob(buf []byte) (*Job, error) {
	var job Job

	err := json.Unmarshal(buf, &job)
	if err != nil {
		return nil, err
	}

	if job.CreatedAt == 0 {
		job.CreatedAt = util.Nowf()
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

	timeout := job.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(res)
	if err != nil {
		return err
	}

	return s.store.Working().AddElement(time.Now().Add(time.Duration(job.Timeout)*time.Second), job.Jid, buf.Bytes())
}
