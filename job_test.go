package worq

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonParse(t *testing.T) {
	t.Parallel()

	str := "{\"jid\":\"12345678901234567890abcd\",\"class\":\"Thing\",\"args\":[123]}"
	job, err := ParseJob(bytes.NewBufferString(str).Bytes())
	assert.NoError(t, err)
	assert.Equal(t, "12345678901234567890abcd", job.Jid)
	assert.Equal(t, "", job.Gid)
	assert.Equal(t, "Thing", job.Class)
	assert.NotEqual(t, float64(0), job.CreatedAt)
	assert.Equal(t, float64(0), job.EnqueuedAt)

	str = "{\"jid\":\"12345678901234567890abcd\",\"class\":\"Thing\",\"args\":[123],\"created_at\":1496784309.923706863,\"enqueued_at\":1496784309.923706864}"
	job, err = ParseJob(bytes.NewBufferString(str).Bytes())
	assert.NoError(t, err)
	assert.Equal(t, "12345678901234567890abcd", job.Jid)
	assert.Equal(t, "Thing", job.Class)
	assert.Equal(t, 1496784309.923706863, job.CreatedAt)
	assert.Equal(t, 1496784309.923706864, job.EnqueuedAt)

}
