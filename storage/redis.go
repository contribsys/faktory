package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"regexp"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

type redisStore struct {
	Name      string
	mu        sync.Mutex
	queueSet  map[string]*redisQueue
	scheduled *redisSorted
	retries   *redisSorted
	dead      *redisSorted
	working   *redisSorted

	rclient *redis.Client
}

func NewRedisStore(name string, rclient *redis.Client) (Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rs := &redisStore{
		Name:     name,
		mu:       sync.Mutex{},
		queueSet: map[string]*redisQueue{},
		rclient:  rclient,
	}
	rs.initSorted()

	vals, err := rs.rclient.SMembers(ctx, "queues").Result()
	if err != nil {
		return nil, err
	}
	for idx := range vals {
		q := rs.NewQueue(vals[idx])
		err := q.init(ctx)
		if err != nil {
			util.Warnf("Unable to initialize queue: %v", err)
			continue
		}
		rs.queueSet[vals[idx]] = q
	}
	return rs, nil
}

var (
	instances  = map[string]*exec.Cmd{}
	redisMutex = sync.Mutex{}
)

func bootRedis(path string, sock string) (func(), error) {
	redisMutex.Lock()
	defer redisMutex.Unlock()
	if _, ok := instances[sock]; ok {
		return func() {
			err := stopRedis(sock)
			if err != nil {
				util.Error("Unable to stop Redis", err)
			}
		}, nil
	}
	util.Infof("Initializing redis storage at %s, socket %s", path, sock)

	err := os.MkdirAll(path, os.ModeDir|0o755)
	if err != nil {
		return nil, err
	}

	rclient := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    sock,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = rclient.Ping(ctx).Result()
	if err != nil {
		// util.Debugf("Redis not alive, booting... -- %s", err)

		conffilename := "/tmp/redis.conf"
		if _, err := os.Stat(conffilename); err != nil {
			if err != nil && os.IsNotExist(err) {
				// nolint:gosec
				err := os.WriteFile("/tmp/redis.conf", []byte(fmt.Sprintf(redisconf, client.Version)), 0o444)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		binary, err := exec.LookPath("redis-server")
		if err != nil {
			return nil, err
		}

		logfile := fmt.Sprintf("%s/redis.log", path)
		loglevel := "warning"
		if util.LogDebug {
			loglevel = "notice"
		}
		arguments := []string{
			binary,
			conffilename,
			"--unixsocket",
			sock,
			"--loglevel",
			loglevel,
			"--dir",
			path,
			"--logfile",
			logfile,
		}

		util.Debugf("Booting Redis: %s", strings.Join(arguments, " "))

		// nolint:gosec
		cmd := exec.Command(arguments[0], arguments[1:]...)
		util.EnsureChildShutdown(cmd, util.SIGTERM) // platform-specific tuning
		// cmd.Stdout = os.Stdout
		// cmd.Stderr = os.Stderr
		instances[sock] = cmd
		err = cmd.Start()
		if err != nil {
			return nil, err
		}

		// wait a few seconds for Redis to start
		start := time.Now()
		for i := 0; i < 1000; i++ {
			conn, err := net.Dial("unix", sock)
			if err == nil {
				conn.Close()
				break
			}

			time.Sleep(10 * time.Millisecond)
		}
		done := time.Now()
		util.Debugf("Redis booted in %s", done.Sub(start))

		// This will panic and crash Faktory if Redis dies for some reason.
		go func() {
			err := cmd.Wait()
			if err != nil {
				util.Warnf("Redis at %s crashed: %s", path, err)
				panic(err)
			}
		}()
	}

	secs := 600
	for {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		_, err = rclient.Ping(ctx).Result()
		cancel()
		if err == nil {
			break
		} else if secs == 0 {
			return nil, err
		} else if strings.HasPrefix(err.Error(), "LOADING") {
			secs -= 1
			util.Info("Faktory is waiting for Redis to load...")
			time.Sleep(1 * time.Second)
		} else {
			return nil, err
		}
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	infos, err := rclient.Info(ctx).Result()
	if err != nil {
		return nil, err
	}
	version := "Unknown"
	scanner := bufio.NewScanner(bytes.NewBufferString(infos))
	for scanner.Scan() {
		txt := scanner.Text()
		if strings.Contains(txt, "redis_version") {
			version = strings.Split(txt, ":")[1]
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	util.Debugf("Running Redis v%s", version)
	err = rclient.Close()
	if err != nil {
		return nil, err
	}

	return func() {
		err := stopRedis(sock)
		if err != nil {
			util.Error("Unable to stop Redis", err)
		}
	}, nil
}

func openRedis(sock string, poolSize int) (Store, error) {
	redisMutex.Lock()
	defer redisMutex.Unlock()
	if _, ok := instances[sock]; !ok {
		return nil, errors.New("redis not booted, cannot start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rclient := redis.NewClient(&redis.Options{
		Network:  "unix",
		Addr:     sock,
		DB:       0,
		PoolSize: poolSize,
	})
	_, err := rclient.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	return NewRedisStore(sock, rclient)
}

func (store *redisStore) Stats(ctx context.Context) map[string]string {
	return map[string]string{
		"stats": store.rclient.Info(ctx).String(),
		"name":  store.Name,
	}
}

func (store *redisStore) PausedQueues(ctx context.Context) ([]string, error) {
	return store.rclient.SMembers(ctx, "paused").Result()
}

// queues are iterated in sorted, lexigraphical order
func (store *redisStore) EachQueue(ctx context.Context, x func(Queue)) {
	for _, k := range store.queueSet {
		x(k)
	}
}

func (store *redisStore) Flush(ctx context.Context) error {
	return store.rclient.FlushDB(ctx).Err()
}

var (
	ValidQueueName = regexp.MustCompile(`\A[a-zA-Z0-9._-]+\z`)
)

// returns an existing, known queue or nil
func (store *redisStore) ExistingQueue(_ context.Context, name string) (Queue, bool) {
	q, ok := store.queueSet[name]
	return q, ok
}

// creates the queue if it doesn't already exist
func (store *redisStore) GetQueue(ctx context.Context, name string) (Queue, error) {
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

	q = store.NewQueue(name)
	err := q.init(ctx)
	if err != nil {
		return nil, err
	}
	err = store.rclient.SAdd(ctx, "queues", name).Err()
	if err != nil {
		return nil, fmt.Errorf("Unable to store queue name: %v", err)
	}
	store.queueSet[name] = q
	return q, nil
}

func (store *redisStore) Close() error {
	util.Debug("Stopping storage")
	store.mu.Lock()
	defer store.mu.Unlock()

	return store.rclient.Close()
}

func (store *redisStore) Redis() *redis.Client {
	return store.rclient
}

func stopRedis(sock string) error {
	redisMutex.Lock()
	defer redisMutex.Unlock()

	cmd, ok := instances[sock]
	if !ok {
		return errors.New("No such redis instance " + sock)
	}

	util.Debugf("Shutting down Redis PID %d", cmd.Process.Pid)
	before := time.Now()
	p := cmd.Process
	pid := p.Pid

	// this call frequently errs and the returned error is a string,
	// not easy to build logic around and too noisy to log
	// "os: process already finished"
	// TODO revisit error handling in versions after Go 1.14.
	_ = p.Signal(syscall.SIGTERM)
	delete(instances, sock)

	// Test suite hack: Redis will not exit if we
	// don't give it enough time to reopen the RDB
	// file before deleting the entire storage directory.
	// time.Sleep(100 * time.Millisecond)
	i := 500
	for ; i > 0; i-- {
		time.Sleep(2 * time.Millisecond)
		err := syscall.Kill(pid, syscall.Signal(0))
		if errors.Is(err, syscall.ESRCH) {
			util.Debugf("Redis dead in %v", time.Since(before))
			return nil
		} else {
			return err
		}
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

func (store *redisStore) EnqueueAll(ctx context.Context, sset SortedSet) error {
	return sset.Each(ctx, func(_ int, entry SortedEntry) error {
		j, err := entry.Job()
		if err != nil {
			return err
		}

		k, err := entry.Key()
		if err != nil {
			return err
		}

		q, err := store.GetQueue(ctx, j.Queue)
		if err != nil {
			return err
		}

		ok, err := sset.Remove(ctx, k)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		return q.Add(ctx, j)
	})
}

func (store *redisStore) EnqueueFrom(ctx context.Context, sset SortedSet, key []byte) error {
	entry, err := sset.Get(ctx, key)
	if err != nil {
		return err
	}
	if entry == nil {
		// race condition, element was removed already
		return nil
	}

	job, err := entry.Job()
	if err != nil {
		return err
	}

	q, err := store.GetQueue(ctx, job.Queue)
	if err != nil {
		return err
	}

	ok, err := sset.Remove(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return q.Add(ctx, job)
}

var (
	Open = openRedis
	Boot = bootRedis
	Stop = stopRedis
)

const (
	redisconf = `
# DO NOT EDIT
# Created by Faktory %s
bind 127.0.0.1 ::1
port 0

# Faktory's redis is only available via local Unix socket.
# This maximizes performance and minimizes opsec risk.
unixsocket /tmp/faktory-redis.sock
unixsocketperm 700
timeout 0

daemonize no
maxmemory-policy noeviction

# Specify the server verbosity level.
# This can be one of:
# debug (a lot of information, useful for development/testing)
# verbose (many rarely useful info, but not a mess like the debug level)
# notice (moderately verbose, what you want in production probably)
# warning (only very important / critical messages are logged)
loglevel notice
logfile /tmp/faktory-redis.log

# we're pretty aggressive on persistence to minimize data loss.
# remember you can take backups of the RDB file with a simple 'cp'.
save 120 1
save 30 5
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename faktory.rdb
slowlog-log-slower-than 10000
slowlog-max-len 128
`
)
