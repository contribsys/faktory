package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnectionsSummary(t *testing.T) {
	t.Parallel()

	now := time.Now()
	worker := &ClientData{Hostname: "worker-1", Pid: 10, Wid: "abc123", Labels: []string{"ruby"}}
	producer := &ClientData{Hostname: "web-1", Pid: 20}
	anon := &ClientData{}

	conns := newConnections()
	assert.Empty(t, conns.summary())

	all := []*Connection{
		{Client: worker, address: "10.0.0.1", startedAt: now},
		{Client: worker, address: "10.0.0.1", startedAt: now.Add(-time.Hour)},
		{Client: producer, address: "10.0.0.2", startedAt: now},
		{Client: producer, address: "10.0.0.2", startedAt: now},
		{Client: producer, address: "10.0.0.2", startedAt: now.Add(-time.Minute)},
		// clients which don't identify themselves are grouped by remote IP
		{Client: anon, address: "10.0.0.3", startedAt: now},
		{Client: &ClientData{}, address: "10.0.0.3", startedAt: now},
		{Client: &ClientData{}, address: "10.0.0.4", startedAt: now},
	}
	for _, cn := range all {
		conns.add(cn)
	}

	sum := conns.summary()
	assert.Len(t, sum, 4)

	// workers sort first, then by connection count
	assert.True(t, sum[0].IsConsumer())
	assert.Equal(t, "abc123", sum[0].Wid)
	assert.Equal(t, 2, sum[0].Connections)
	assert.Equal(t, now.Add(-time.Hour), sum[0].Since)
	assert.Equal(t, []string{"ruby"}, sum[0].Labels)

	assert.False(t, sum[1].IsConsumer())
	assert.Equal(t, "web-1", sum[1].Hostname)
	assert.Equal(t, 20, sum[1].Pid)
	assert.Equal(t, "10.0.0.2", sum[1].Address)
	assert.Equal(t, 3, sum[1].Connections)
	assert.Equal(t, now.Add(-time.Minute), sum[1].Since)

	assert.Equal(t, "10.0.0.3", sum[2].Address)
	assert.Equal(t, 2, sum[2].Connections)
	assert.Equal(t, "10.0.0.4", sum[3].Address)
	assert.Equal(t, 1, sum[3].Connections)

	conns.remove(all[0])
	conns.remove(all[1])
	conns.remove(all[1])
	sum = conns.summary()
	assert.Len(t, sum, 3)
	assert.False(t, sum[0].IsConsumer())
}

func TestServerTracksConnections(t *testing.T) {
	runServer("localhost:4481", func(s *Server) {
		assert.Empty(t, s.ClientConnections())

		hello := func(payload string) net.Conn {
			conn, err := net.DialTimeout("tcp", "localhost:4481", 1*time.Second)
			assert.NoError(t, err)
			buf := bufio.NewReader(conn)
			_, err = buf.ReadString('\n')
			assert.NoError(t, err)
			_, _ = conn.Write([]byte("HELLO " + payload + "\r\n"))
			result, err := buf.ReadString('\n')
			assert.NoError(t, err)
			assert.Equal(t, "+OK\r\n", result)
			return conn
		}

		p1 := hello(`{"hostname":"web-1","pid":20,"v":2}`)
		p2 := hello(`{"hostname":"web-1","pid":20,"v":2}`)
		w1 := hello(`{"hostname":"worker-1","pid":10,"wid":"abc123","v":2}`)

		sum := s.ClientConnections()
		assert.Len(t, sum, 2)
		assert.Equal(t, "abc123", sum[0].Wid)
		assert.Equal(t, 1, sum[0].Connections)
		assert.Equal(t, "web-1", sum[1].Hostname)
		assert.Equal(t, 2, sum[1].Connections)
		assert.NotEmpty(t, sum[1].Address)

		_, _ = p1.Write([]byte("END\n"))
		_ = p1.Close()
		assert.Eventually(t, func() bool {
			sum := s.ClientConnections()
			return len(sum) == 2 && sum[1].Connections == 1
		}, time.Second, 10*time.Millisecond)

		_ = p2.Close()
		_ = w1.Close()
		assert.Eventually(t, func() bool {
			return len(s.ClientConnections()) == 0
		}, time.Second, 10*time.Millisecond)
	})
}
