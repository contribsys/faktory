package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	ZERO = int64(0)
	ONE  = int64(1)
	TWO  = int64(2)
)

func TestScheduler(t *testing.T) {
	fa := &fakeAdapter{}
	s := NewScheduler("retries", fa)

	data := s.Stats()
	assert.NotNil(t, data)
	assert.Equal(t, ZERO, data["enqueued"])
	assert.Equal(t, ZERO, data["cycles"])

	count := s.cycle()

	data = s.Stats()
	assert.NotNil(t, data)
	assert.Equal(t, ZERO, count)
	assert.Equal(t, ZERO, data["enqueued"])
	assert.Equal(t, ONE, data["cycles"])

	fa.sorted = [][]byte{[]byte(`{"queue":"default"}`), []byte(`{"queue":"bob"}`)}
	count = s.cycle()

	data = s.Stats()
	assert.NotNil(t, data)
	assert.Equal(t, TWO, count)
	assert.Equal(t, TWO, data["enqueued"])
	assert.Equal(t, TWO, data["cycles"])
	assert.Equal(t, 2, fa.QueueSize())

	s.Stop()
}

type fakeAdapter struct {
	sorted [][]byte
	queue  [][]byte
}

func (fs *fakeAdapter) Prune(ts string) ([][]byte, error) {
	return fs.sorted, nil
}
func (fs *fakeAdapter) Push(name string, data []byte) error {
	fs.queue = append(fs.queue, data)
	return nil
}

func (fs *fakeAdapter) QueueSize() int {
	return len(fs.queue)
}
