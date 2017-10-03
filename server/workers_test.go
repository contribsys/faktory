package server

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientWorker(t *testing.T) {
	t.Parallel()

	cw, err := clientWorkerFromAhoy("")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientWorkerFromAhoy("{")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientWorkerFromAhoy("{}")
	assert.Error(t, err)
	assert.Nil(t, cw)

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	cw, err = clientWorkerFromAhoy(ahoy)
	assert.NoError(t, err)
	assert.NotNil(t, cw)

	assert.Equal(t, "", cw.state)
	assert.False(t, cw.IsQuiet())

	cw.Signal("quiet")
	assert.Equal(t, "quiet", cw.state)
	assert.True(t, cw.IsQuiet())

	cw.Signal("terminate")
	assert.Equal(t, "terminate", cw.state)
	assert.True(t, cw.IsQuiet())

	assert.Equal(t, 0, cw.BusyCount())
}

func TestHeartbeats(t *testing.T) {
	beatsByMe := map[string]*ClientWorker{}
	mu := sync.Mutex{}

	assert.Equal(t, 0, len(beatsByMe))
	reapHeartbeats(beatsByMe, nil)

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	client, err := clientWorkerFromAhoy(ahoy)
	assert.NoError(t, err)

	before := time.Now()
	updateHeartbeat(client, beatsByMe, &mu)
	after := time.Now()
	assert.True(t, client.lastHeartbeat.After(before))
	assert.True(t, client.lastHeartbeat.Before(after))

	assert.Equal(t, 1, len(beatsByMe))
	reapHeartbeats(beatsByMe, nil)
	assert.Equal(t, 1, len(beatsByMe))

	updateHeartbeat(client, beatsByMe, &mu)
	assert.False(t, client.lastHeartbeat.Before(after))

	client.lastHeartbeat = time.Now().Add(-65 * time.Second)
	reapHeartbeats(beatsByMe, &mu)
	assert.Equal(t, 0, len(beatsByMe))
}
