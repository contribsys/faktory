package server

import (
	"sync"
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

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	cw, err = clientDataFromHello(ahoy)
	assert.NoError(t, err)
	assert.NotNil(t, cw)

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

	assert.Equal(t, 0, cw.BusyCount())
}

func TestHeartbeats(t *testing.T) {
	t.Parallel()

	beatsByMe := map[string]*ClientData{}
	var mu sync.RWMutex

	assert.Equal(t, 0, len(beatsByMe))
	reapHeartbeats(beatsByMe, &mu)

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	client, err := clientDataFromHello(ahoy)
	assert.NoError(t, err)

	before := time.Now()
	updateHeartbeat(client, beatsByMe, &mu)
	after := time.Now()
	assert.True(t, client.lastHeartbeat.After(before))
	assert.True(t, client.lastHeartbeat.Before(after))

	assert.Equal(t, 1, len(beatsByMe))
	reapHeartbeats(beatsByMe, &mu)
	assert.Equal(t, 1, len(beatsByMe))

	updateHeartbeat(client, beatsByMe, &mu)
	assert.False(t, client.lastHeartbeat.Before(after))

	client.lastHeartbeat = time.Now().Add(-65 * time.Second)
	reapHeartbeats(beatsByMe, &mu)
	assert.Equal(t, 0, len(beatsByMe))
}
