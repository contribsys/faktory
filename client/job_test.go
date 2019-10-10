package client

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJob(t *testing.T) {
	job := NewJob("yo", 1)
	assert.EqualValues(t, 25, job.Retry)
	data, err := json.Marshal(job)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "retry")
}

func TestJobCustomOptions(t *testing.T) {
	job := NewJob("yo", 1)
	expiresAt := time.Now().Add(1 * time.Hour)
	job.SetUniqueFor(100).
		SetUniqueUntil(UniqueUntilStart).
		SetExpiresAt(expiresAt)

	assert.EqualValues(t, 100, job.Custom["unique_for"])
	assert.EqualValues(t, UniqueUntilStart, job.Custom["unique_until"])
	assert.EqualValues(t, expiresAt.Format(ISO8601), job.Custom["expires_at"])
}
