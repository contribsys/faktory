package client

import (
	cryptorand "crypto/rand"
	"encoding/base64"
	"fmt"
	"time"
)

type UniqueUntil string

var (
	RetryPolicyDefault        = 25
	RetryPolicyEmphemeral     = 0
	RetryPolicyDirectToMorgue = -1
)

const (
	UntilSuccess UniqueUntil = "success" // default
	UntilStart   UniqueUntil = "start"
)

type Failure struct {
	RetryCount     int      `json:"retry_count"`
	RetryRemaining int      `json:"remaining"`
	FailedAt       string   `json:"failed_at"`
	NextAt         string   `json:"next_at,omitempty"`
	ErrorMessage   string   `json:"message,omitempty"`
	ErrorType      string   `json:"errtype,omitempty"`
	Backtrace      []string `json:"backtrace,omitempty"`
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
	Retry      *int                   `json:"retry"`
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
		Jid:       RandomJid(),
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Retry:     &RetryPolicyDefault,
	}
}

func RandomJid() string {
	bytes := make([]byte, 12)
	_, err := cryptorand.Read(bytes)
	if err != nil {
		panic(fmt.Errorf("unable to read random bytes: %w", err))
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

// Set custom metadata for this job. Faktory reserves all
// element names starting with "_" for internal use, e.g.
// SetCustom("_txid", "12345")
func (j *Job) SetCustom(name string, value interface{}) *Job {
	if j.Custom == nil {
		j.Custom = map[string]interface{}{}
	}

	j.Custom[name] = value
	return j
}

////////////////////////////////////////////
// Faktory Enterprise helpers
//
// These helpers allow you to configure several Faktory Enterprise features.
// They will have no effect unless you are running Faktory Enterprise.

// Configure this job to be unique for +secs+ seconds or until the job
// has been successfully processed.
func (j *Job) SetUniqueFor(secs uint) *Job {
	return j.SetCustom("unique_for", secs)
}

// Configure the uniqueness deadline for this job, legal values
// are:
//
//   - "success" - the job will be considered unique until it has successfully processed
//     or the +unique_for+ TTL has passed, this is the default value.
//   - "start" - the job will be considered unique until it starts processing. Retries
//     may lead to multiple copies of the job running.
func (j *Job) SetUniqueness(until UniqueUntil) *Job {
	return j.SetCustom("unique_until", until)
}

// Configure the TTL for this job. After this point in time, the job will be
// discarded rather than executed.
func (j *Job) SetExpiresAt(expiresAt time.Time) *Job {
	return j.SetCustom("expires_at", expiresAt.Format(time.RFC3339Nano))
}

func (j *Job) SetExpiresIn(expiresIn time.Duration) *Job {
	return j.SetCustom("expires_at", time.Now().Add(expiresIn).Format(time.RFC3339Nano))
}
