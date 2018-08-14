package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"regexp"

	"github.com/contribsys/faktory/util"
	"github.com/go-redis/redis"
)

type redisStore struct {
	Name      string
	mu        sync.Mutex
	history   *processingHistory
	queueSet  map[string]*redisQueue
	scheduled *redisSorted
	retries   *redisSorted
	dead      *redisSorted
	working   *redisSorted

	client    *redis.Client
	redisPath string
	redisPid  int
	redisPort int
	conffile  string
}

func OpenRedis(path string) (Store, error) {
	util.LogInfo = true

	util.Infof("Initializing storage at %s", path)

	err := os.MkdirAll(path, os.ModeDir|0755)
	if err != nil {
		return nil, err
	}

	rs := &redisStore{
		Name:     path,
		mu:       sync.Mutex{},
		history:  &processingHistory{},
		queueSet: map[string]*redisQueue{},
	}
	rs.initSorted()

	rs.redisPath = path
	rs.redisPort = rand.Int() % 65535

	sock := fmt.Sprintf("/tmp/faktory-redis.%d.sock", rs.redisPort)
	_ = os.Remove(sock)
	err = rs.start()
	if err != nil {
		return nil, err
	}

	// wait a few seconds for Redis to start
	for i := 0; i < 50; i++ {
		f, err := os.Open(sock)
		if err == nil {
			f.Close()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	rs.client = redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    sock,
	})
	_, err = rs.client.Ping().Result()
	if err != nil {
		return nil, err
	}

	infos, err := rs.client.Info().Result()
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewBufferString(infos))
	for scanner.Scan() {
		txt := scanner.Text()
		if strings.Index(txt, "redis_version") != -1 {
			data := strings.Split(txt, ":")
			util.Infof("Using redis %s", data[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rs, nil
}

func (store *redisStore) Stats() map[string]string {
	return map[string]string{
		"stats": store.client.Info().String(),
		"name":  fmt.Sprintf("/tmp/faktory-redis.%d.sock", store.redisPort),
	}
}

func (store *redisStore) Processed() uint64 {
	return uint64(store.client.IncrBy("processed", 0).Val())
}

func (store *redisStore) Failures() uint64 {
	return uint64(store.client.IncrBy("failures", 0).Val())
}

// queues are iterated in sorted, lexigraphical order
func (store *redisStore) EachQueue(x func(Queue)) {
	for _, k := range store.queueSet {
		x(k)
	}
}

func (store *redisStore) Compact() error {
	return nil
}

func (store *redisStore) Flush() error {
	return store.client.FlushDB().Err()
}

var (
	ValidQueueName = regexp.MustCompile(`\A[a-zA-Z0-9._-]+\z`)
)

func (store *redisStore) GetQueue(name string) (Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("queue name cannot be blank")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	q, ok := store.queueSet[name]
	if ok {
		return q, nil
	}

	if !ValidQueueName.MatchString(name) {
		return nil, fmt.Errorf("queue names must match %v", ValidQueueName)
	}

	q = &redisQueue{
		name: name,
	}
	err := q.init()
	if err != nil {
		return nil, err
	}
	store.queueSet[name] = q
	return q, nil
}

func (store *redisStore) Close() error {
	util.Info("Stopping storage")
	store.mu.Lock()
	defer store.mu.Unlock()

	p, err := os.FindProcess(store.redisPid)
	if err != nil {
		return err
	}
	err = p.Signal(syscall.SIGTERM)
	if err != nil {
		return err
	}
	_, err = p.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (store *redisStore) Retries() SortedSet {
	return store.retries
}

func (store *redisStore) Scheduled() SortedSet {
	return store.scheduled
}

func (store *redisStore) Working() SortedSet {
	return store.working
}

func (store *redisStore) Dead() SortedSet {
	return store.dead
}

func (rs *redisStore) start() error {
	conffilename := "/tmp/redis.conf"
	//conf, _ := os.Open(conffilename, os.O_WRONLY|os.O_CREATE, 0444)
	//ioutil.WriteAll(conf, confByes)
	//conf.Close()

	redisLoc := "/usr/local/bin/redis-server"
	loglevel := "notice"
	if util.LogDebug {
		loglevel = "verbose"
	}
	arguments := []string{
		"/usr/local/bin/redis-server",
		conffilename,
		"--unixsocket",
		fmt.Sprintf("/tmp/faktory-redis.%d.sock", rs.redisPort),
		"--port",
		fmt.Sprintf("%d", rs.redisPort),
		"--loglevel",
		loglevel,
		"--dir",
		rs.redisPath,
	}

	pid, err := syscall.ForkExec(redisLoc, arguments, nil)
	if err != nil {
		return err
	}
	rs.redisPid = pid
	rs.conffile = conffilename

	return nil
}
