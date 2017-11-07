package faktory

import (
	cryptorand "crypto/rand"
	"encoding/base64"
	mathrand "math/rand"

	"github.com/contribsys/faktory/util"
)

type Failure struct {
	RetryCount   int      `json:"retry_count"`
	FailedAt     string   `json:"failed_at"`
	NextAt       string   `json:"next_at,omitempty"`
	ErrorMessage string   `json:"message,omitempty"`
	ErrorType    string   `json:"errtype,omitempty"`
	Backtrace    []string `json:"backtrace,omitempty"`
}

type Job struct {
	// required
	Jid   string        `json:"jid"`
	Queue string        `json:"queue"`
	Type  string        `json:"jobtype"`
	Args  []interface{} `json:"args"`

	// optional
	Priority   uint8                  `json:"priority,omitempty"`
	CreatedAt  string                 `json:"created_at,omitempty"`
	EnqueuedAt string                 `json:"enqueued_at,omitempty"`
	At         string                 `json:"at,omitempty"`
	ReserveFor int                    `json:"reserve_for,omitempty"`
	Retry      int                    `json:"retry,omitempty"`
	Backtrace  int                    `json:"backtrace,omitempty"`
	Failure    *Failure               `json:"failure,omitempty"`
	Custom     map[string]interface{} `json:"custom,omitempty"`
}

func NewJob(jobtype string, args ...interface{}) *Job {
	return &Job{
		Type:      jobtype,
		Queue:     "default",
		Args:      args,
		Jid:       randomJid(),
		CreatedAt: util.Nows(),
		Retry:     25,
	}
}

func (j *Job) EnsureValidPriority() {
	// Priority can never be negative because of signedness
	if j.Priority > 9 {
		// set to 0 so we use the default value in GetPriority
		j.Priority = 0
	}
}

// Accessor so that if priority isn't specified we don't persist it to disk
func (j *Job) GetPriority() uint8 {
	if j.Priority == 0 {
		return 5
	}
	return j.Priority
}

func randomJid() string {
	bytes := make([]byte, 12)
	_, err := cryptorand.Read(bytes)
	if err != nil {
		mathrand.Read(bytes)
	}

	return base64.RawURLEncoding.EncodeToString(bytes)
}
