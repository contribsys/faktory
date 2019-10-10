package client

import (
	cryptorand "crypto/rand"
	"encoding/base64"
	mathrand "math/rand"
	"time"
)

const ISO8601 = "2006-01-02T15:04:05Z"

type UniqueUntil string

const (
	UniqueUntilSuccess UniqueUntil = "success"
	UniqueUntilStart   UniqueUntil = "start"
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
	CreatedAt  string                 `json:"created_at,omitempty"`
	EnqueuedAt string                 `json:"enqueued_at,omitempty"`
	At         string                 `json:"at,omitempty"`
	ReserveFor int                    `json:"reserve_for,omitempty"`
	Retry      int                    `json:"retry"`
	Backtrace  int                    `json:"backtrace,omitempty"`
	Failure    *Failure               `json:"failure,omitempty"`
	Custom     map[string]interface{} `json:"custom,omitempty"`
}

// Clients should use this constructor to build a Job, not allocate
// a bare struct directly.
func NewJob(jobtype string, args ...interface{}) *Job {
	return &Job{
		Type:      jobtype,
		Queue:     "default",
		Args:      args,
		Jid:       randomJid(),
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Retry:     25,
	}
}

func randomJid() string {
	bytes := make([]byte, 12)
	_, err := cryptorand.Read(bytes)
	if err != nil {
		mathrand.Read(bytes)
	}

	return base64.RawURLEncoding.EncodeToString(bytes)
}

func (j *Job) GetCustom(name string) (interface{}, bool) {
	if j.Custom == nil {
		return nil, false
	}

	val, ok := j.Custom[name]
	return val, ok
}

func (j *Job) SetCustom(name string, value interface{}) {
	if j.Custom == nil {
		j.Custom = map[string]interface{}{}
	}

	j.Custom[name] = value
}

func (j *Job) SetUniqueFor(seconds uint) *Job {
	j.SetCustom("unique_for", seconds)
	return j
}

func (j *Job) SetUniqueUntil(until UniqueUntil) *Job {
	j.SetCustom("unique_until", until)
	return j
}

func (j *Job) SetExpiresAt(expiresAt time.Time) *Job {
	j.SetCustom("expires_at", expiresAt.Format(ISO8601))
	return j
}
