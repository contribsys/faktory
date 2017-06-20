package worq

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJsonParse(t *testing.T) {
	t.Parallel()

	str := "{\"jid\":\"12345678901234567890abcd\",\"jobtype\":\"Thing\",\"args\":[123]}"
	job, err := ParseJob(bytes.NewBufferString(str).Bytes())
	assert.NoError(t, err)
	assert.Equal(t, "12345678901234567890abcd", job.Jid)
	assert.Equal(t, "", job.Gid)
	assert.Equal(t, "Thing", job.Type)
	assert.NotEqual(t, "", job.CreatedAt)
	assert.Equal(t, "", job.EnqueuedAt)

	str = "{\"jid\":\"12345678901234567890abcd\",\"jobtype\":\"Thing\",\"args\":[123],\"created_at\":\"2006-01-02T15:04:05.001000Z\",\"enqueued_at\":\"2006-01-02T15:04:05.001001Z\"}"
	job, err = ParseJob(bytes.NewBufferString(str).Bytes())
	assert.NoError(t, err)
	assert.Equal(t, "12345678901234567890abcd", job.Jid)
	assert.Equal(t, "Thing", job.Type)
	assert.Equal(t, "2006-01-02T15:04:05.001000Z", job.CreatedAt)
	assert.Equal(t, "2006-01-02T15:04:05.001001Z", job.EnqueuedAt)

}
