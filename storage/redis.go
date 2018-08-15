package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
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
	queueSet  map[string]*redisQueue
	scheduled *redisSorted
	retries   *redisSorted
	dead      *redisSorted
	working   *redisSorted

	client *redis.Client
	DB     int
}

var (
	redisPort = 0
	redisPid  = 0
	redisPath = ""
	redisConf = ""
	redisSock = ""
	opens     = 0
	dbs       = map[int]bool{}
	dbLock    = sync.Mutex{}
)

func MustBootRedis() {
	datapath := os.Getenv("FAKTORY_REDIS_PATH")
	if datapath == "" {
		datapath = "/var/lib/faktory/default"
	}
	port := os.Getenv("FAKTORY_REDIS_PORT")
	if port == "" {
		port = "7421"
	}
	iport, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	BootRedis(datapath, iport)
}

func BootRedis(path string, port int) {
	util.LogInfo = true
	util.LogDebug = true
	util.Infof("Initializing redis storage at %s on port %d", path, port)

	err := os.MkdirAll(path, os.ModeDir|0755)
	if err != nil {
		panic(err)
	}

	sock := fmt.Sprintf("/tmp/faktory-redis.%d.sock", port)
	_ = os.Remove(sock)

	conffilename := "/tmp/redis.conf"
	redisLoc := "/usr/local/bin/redis-server"
	loglevel := "notice"
	if util.LogDebug {
		loglevel = "verbose"
	}
	arguments := []string{
		"/usr/local/bin/redis-server",
		conffilename,
		"--unixsocket",
		sock,
		"--port",
		fmt.Sprintf("%d", port),
		"--loglevel",
		loglevel,
		"--dir",
		path,
	}

	pid, err := syscall.ForkExec(redisLoc, arguments, nil)
	if err != nil {
		panic(err)
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

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    sock,
	})
	_, err = client.Ping().Result()
	if err != nil {
		panic(err)
	}

	infos, err := client.Info().Result()
	if err != nil {
		panic(err)
	}
	version := "Unknown"
	scanner := bufio.NewScanner(bytes.NewBufferString(infos))
	for scanner.Scan() {
		txt := scanner.Text()
		if strings.Index(txt, "redis_version") != -1 {
			version = strings.Split(txt, ":")[1]
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	util.Infof("Redis v%s booted in %s", version, done.Sub(start))
	err = client.Close()
	if err != nil {
		panic(err)
	}

	redisPath = path
	redisPort = port
	redisPid = pid
	redisConf = conffilename
	redisSock = sock
}

func OpenRedis() (Store, error) {
	util.LogInfo = true

	if redisPid == 0 {
		return nil, errors.New("redis not started, call MustBootRedis() first")
	}

	// find and reserve an unused DB index
	db := 0
	dbLock.Lock()
	for {
		val := dbs[db]
		if !val {
			dbs[db] = true
			break
		}
		db += 1
	}
	dbLock.Unlock()

	rs := &redisStore{
		Name:     redisPath,
		DB:       db,
		mu:       sync.Mutex{},
		queueSet: map[string]*redisQueue{},
	}
	rs.initSorted()

	rs.client = redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    redisSock,
		DB:      db,
	})
	opens += 1
	return rs, nil
}

func (store *redisStore) Stats() map[string]string {
	return map[string]string{
		"stats": store.client.Info().String(),
		"name":  redisSock,
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

	opens -= 1

	dbLock.Lock()
	dbs[store.DB] = false
	dbLock.Unlock()

	if opens == 0 {
		p, err := os.FindProcess(redisPid)
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

		sock := fmt.Sprintf("/tmp/faktory-redis.%d.sock", redisPort)
		_ = os.Remove(sock)

		redisPid = 0
		redisPort = 0
		redisPath = ""
		redisConf = ""
		redisSock = ""
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
