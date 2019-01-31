package client

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobPriority(t *testing.T) {
	job := NewJob("yo", 1)
	assert.EqualValues(t, 25, job.Retry)
	data, err := json.Marshal(job)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "retry")
}
