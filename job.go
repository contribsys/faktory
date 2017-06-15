package worq

import (
	"encoding/json"
	"time"
)

type Job struct {
	Jid        string        `json:"jid"`
	Gid        string        `json:"gid"`
	Queue      string        `json:"queue"`
	EnqueuedAt int64         `json:"enqueued_at"`
	CreatedAt  int64         `json:"created_at"`
	RetryCount int32         `json:"retry_count"`
	Class      string        `json:"class"`
	Args       []interface{} `json:"args"`
}

func ParseJob(buf []byte) (*Job, error) {
	var job Job

	err := json.Unmarshal(buf, &job)
	if err != nil {
		return nil, err
	}

	if job.CreatedAt == 0 {
		job.CreatedAt = time.Now().UnixNano()
	}
	return &job, nil
}

type Reservation struct {
	Job   *Job
	Since time.Time
	Who   string
}

var workingSet = make(map[string]*Reservation, 128)
