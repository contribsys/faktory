package server

import (
	"github.com/contribsys/faktory/util"
)

// This is the ultimate scalability limitation in Faktory,
// we only allow this many connections to Redis.
var DefaultMaxPoolSize uint64 = 2000

type ServerOptions struct {
	GlobalConfig     map[string]any
	Binding          string
	StorageDirectory string
	RedisSock        string
	ConfigDirectory  string
	Environment      string
	Password         string
	WebUIPassword    string
	PoolSize         uint64
}

func (so *ServerOptions) String(subsys string, key string, defval string) string {
	val := so.Config(subsys, key, defval)
	str, ok := val.(string)
	if !ok {
		util.Warnf("Config error: %s/%s is not a String", subsys, key)
		return defval
	}
	return str
}

func (so *ServerOptions) Config(subsys string, key string, defval any) any {
	mapp, ok := so.GlobalConfig[subsys]
	if !ok {
		return defval
	}

	maps, ok := mapp.(map[string]any)
	if !ok {
		util.Warnf("Invalid configuration, expected a %s subsystem, using default", subsys)
		return defval
	}

	val, ok := maps[key]
	if !ok {
		return defval
	}
	return val
}
