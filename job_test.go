package faktory

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobPriority(t *testing.T) {
	job := Job{}
	assert.Equal(t, uint8(0), job.Priority)
	data, err := json.Marshal(job)
	assert.NoError(t, err)
	assert.NotContains(t, string(data), "priority")

	// reset to 0 and make sure accessor works
	job.Priority = 10
	job.EnsureValidPriority()
	assert.Equal(t, uint8(0), job.Priority)
	assert.Equal(t, uint8(5), job.GetPriority())

	// valid
	job.Priority = 3
	job.EnsureValidPriority()
	assert.Equal(t, uint8(3), job.Priority)
	assert.Equal(t, uint8(3), job.GetPriority())
}
