package server

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientData(t *testing.T) {
	t.Parallel()

	cw, err := clientDataFromHello("")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{}")
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.False(t, cw.IsConsumer())

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	cw, err = clientDataFromHello(ahoy)
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.True(t, cw.IsConsumer())

	assert.Equal(t, Running, cw.state)
	assert.False(t, cw.IsQuiet())

	cw.Signal(Quiet)
	assert.Equal(t, Quiet, cw.state)
	assert.True(t, cw.IsQuiet())

	cw.Signal(Terminate)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())

	// can't go back to quiet
	cw.Signal(Quiet)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())
}

func TestWorkers(t *testing.T) {
	t.Parallel()

	workers := newWorkers()
	assert.Equal(t, 0, workers.Count())

	beat := &ClientBeat{
		Wid: "78629a0f5f3f164f",
	}
	entry, ok := workers.heartbeat(beat)
	assert.Equal(t, 0, workers.Count())
	assert.Nil(t, entry)
	assert.False(t, ok)

	client := &ClientData{
		Hostname:    "MikeBookPro.local",
		Wid:         "78629a0f5f3f164f",
		connections: map[io.Closer]bool{},
	}
	entry, ok = workers.setupHeartbeat(client, &cls{})
	assert.NotNil(t, entry)
	assert.False(t, ok)

	entry, ok = workers.heartbeat(beat)
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)

	before := time.Now()
	entry, ok = workers.heartbeat(beat)
	after := time.Now()
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)
	assert.LessOrEqual(t, before, entry.lastHeartbeat)
	assert.LessOrEqual(t, entry.lastHeartbeat, after)

	assert.Equal(t, Running, entry.state)
	beat.CurrentState = "quiet"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)
	assert.True(t, entry.IsQuiet())

	beat.CurrentState = ""
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)

	beat.CurrentState = "terminate"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Terminate, entry.state)

	count := workers.reapHeartbeats(client.lastHeartbeat)
	assert.Equal(t, 1, workers.Count())
	assert.Equal(t, 0, count)

	count = workers.reapHeartbeats(time.Now())
	assert.Equal(t, 0, workers.Count())
	assert.Equal(t, 1, count)
}

type cls struct{}

func (c cls) Close() error {
	return nil
}
