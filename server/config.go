package server

import "github.com/contribsys/faktory/util"

var DefaultMaxPoolSize = 1000

type ServerOptions struct {
	Binding          string
	StorageDirectory string
	RedisSock        string
	ConfigDirectory  string
	Environment      string
	Password         string
	PoolSize         int
	GlobalConfig     map[string]interface{}
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

func (so *ServerOptions) Config(subsys string, key string, defval interface{}) interface{} {
	mapp, ok := so.GlobalConfig[subsys]
	if !ok {
		return defval
	}

	maps, ok := mapp.(map[string]interface{})
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
