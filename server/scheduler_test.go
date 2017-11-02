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

func TestScanner(t *testing.T) {
	fa := &fakeAdapter{}
	s := &scanner{name: "retries", adapter: fa}

	data := s.Stats()
	assert.NotNil(t, data)
	assert.Equal(t, ZERO, data["enqueued"])
	assert.Equal(t, ZERO, data["cycles"])

	err := s.Execute()
	assert.NoError(t, err)

	data = s.Stats()
	assert.NotNil(t, data)
	assert.Equal(t, ZERO, data["enqueued"])
	assert.Equal(t, ONE, data["cycles"])

	fa.sorted = [][]byte{[]byte(`{"queue":"default"}`), []byte(`{"queue":"bob"}`)}
	err = s.Execute()

	data = s.Stats()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, TWO, data["enqueued"])
	assert.Equal(t, TWO, data["cycles"])
	assert.Equal(t, TWO, fa.Size())
}

type fakeAdapter struct {
	sorted [][]byte
	queue  [][]byte
}

func (fs *fakeAdapter) Prune(ts string) ([][]byte, error) {
	return fs.sorted, nil
}
func (fs *fakeAdapter) Push(name string, _ uint64, data []byte) error {
	fs.queue = append(fs.queue, data)
	return nil
}
func (fs *fakeAdapter) Size() int64 {
	return int64(len(fs.queue))
}
