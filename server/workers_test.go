package server

import (
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

	client := &ClientData{
		Hostname: "MikeBookPro.local",
		Wid:      "78629a0f5f3f164f",
	}

	entry, ok := workers.heartbeat(client, false)
	assert.Equal(t, 0, workers.Count())
	assert.Nil(t, entry)
	assert.False(t, ok)

	entry, ok = workers.heartbeat(client, true)
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)

	before := time.Now()
	entry, ok = workers.heartbeat(client, true)
	after := time.Now()
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)
	assert.True(t, entry.lastHeartbeat.After(before))
	assert.True(t, entry.lastHeartbeat.Before(after))

	count := workers.reapHeartbeats(client.lastHeartbeat)
	assert.Equal(t, 1, workers.Count())
	assert.Equal(t, 0, count)

	count = workers.reapHeartbeats(time.Now())
	assert.Equal(t, 0, workers.Count())
	assert.Equal(t, 1, count)
}
