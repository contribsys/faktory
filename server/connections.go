package server

import (
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Tracks every authenticated network connection, whether it comes
// from a worker process or a plain client which only produces jobs.
// The workers heartbeat map only knows about consumers so this is
// the only place producers are visible.
type connections struct {
	live map[*Connection]struct{}
	mu   sync.Mutex
}

func newConnections() *connections {
	return &connections{
		live: map[*Connection]struct{}{},
	}
}

func (c *connections) add(cn *Connection) {
	c.mu.Lock()
	c.live[cn] = struct{}{}
	c.mu.Unlock()
}

func (c *connections) remove(cn *Connection) {
	c.mu.Lock()
	delete(c.live, cn)
	c.mu.Unlock()
}

// ClientConnections summarizes the connections held by a single
// client process.
type ClientConnections struct {
	// time the oldest live connection was established
	Since    time.Time
	Wid      string
	Hostname string
	// remote IP of the client, as seen by Faktory
	Address     string
	Labels      []string
	Pid         int
	Connections int
}

func (cc *ClientConnections) IsConsumer() bool {
	return cc.Wid != ""
}

// Workers are identified by their WID. Producers have no WID so we
// fall back to hostname, pid and remote IP. Some client libraries don't
// send hostname or pid for producers, those are grouped by remote IP only.
func (c *connections) summary() []*ClientConnections {
	groups := map[string]*ClientConnections{}

	c.mu.Lock()
	for cn := range c.live {
		cl := cn.Client
		key := "w|" + cl.Wid
		if cl.Wid == "" {
			key = "c|" + cl.Hostname + "|" + strconv.Itoa(cl.Pid) + "|" + cn.address
		}
		entry, ok := groups[key]
		if !ok {
			entry = &ClientConnections{
				Since:    cn.startedAt,
				Wid:      cl.Wid,
				Hostname: cl.Hostname,
				Address:  cn.address,
				Labels:   cl.Labels,
				Pid:      cl.Pid,
			}
			groups[key] = entry
		}
		entry.Connections++
		if cn.startedAt.Before(entry.Since) {
			entry.Since = cn.startedAt
		}
	}
	c.mu.Unlock()

	result := make([]*ClientConnections, 0, len(groups))
	for _, entry := range groups {
		result = append(result, entry)
	}
	// workers first, then the heaviest users
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.IsConsumer() != b.IsConsumer() {
			return a.IsConsumer()
		}
		if a.Connections != b.Connections {
			return a.Connections > b.Connections
		}
		if a.Hostname != b.Hostname {
			return a.Hostname < b.Hostname
		}
		if a.Address != b.Address {
			return a.Address < b.Address
		}
		if a.Pid != b.Pid {
			return a.Pid < b.Pid
		}
		return a.Wid < b.Wid
	})
	return result
}

func remoteIP(conn net.Conn) string {
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}
