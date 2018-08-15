package storage

import "os"

func init() {
	err := os.RemoveAll("/tmp/redis-test")
	if err != nil {
		panic(err)
	}
	BootRedis("/tmp/redis-test", 12345)
}
