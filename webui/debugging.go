package webui

import (
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/contribsys/faktory/client"
)

func (dc *PageData) Goroutines() int {
	return runtime.NumGoroutine()
}

func (dc *PageData) NumCPU() int {
	return runtime.NumCPU()
}

func (dc *PageData) MemoryStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func (dc *PageData) RssKb() int64 {
	return client.RssKb()
}

func (dc *PageData) RedisLatency() float64 {
	c := dc.Context()
	redis := c.Store().Redis()
	a := time.Now().UnixNano()
	res := redis.Ping(c.Context)
	b := time.Now().UnixNano()
	_, err := res.Result()
	if err != nil {
		return 0
	}
	return (float64(b-a) / 1000)
}

func (dc *PageData) RedisInfo() string {
	c := dc.Context()
	redis := c.Store().Redis()
	res := redis.Info(c.Context)
	val, err := res.Result()
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	return val
}

func (dc *PageData) Df_h() string {
	cmd := exec.Command("df", "-h")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return err.Error()
	}
	return string(out)
}
