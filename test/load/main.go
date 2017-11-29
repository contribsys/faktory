package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	faktory "github.com/contribsys/faktory/client"
)

var (
	jobs           = int64(30000)
	threads        = int64(10)
	opsCount []int = nil
	queues         = []string{
		"queue0", "queue1", "queue2", "queue3", "queue4",
	}
	pops   = int64(0)
	pushes = int64(0)
)

func main() {
	argc := len(os.Args)
	if argc > 1 {
		aops, err := strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		jobs = aops
	}

	if argc > 2 {
		athreads, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		threads = athreads
	}

	seed := int64(420)
	if argc > 3 {
		aseed, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		seed = aseed
	}

	fmt.Printf("Running loadtest with %d jobs and %d threads\n", jobs, threads)

	client, err := faktory.Open()
	if err != nil {
		handleError(err)
		return
	}
	defer client.Close()
	client.Flush()

	rand.Seed(int64(seed))
	opsCount = make([]int, threads)
	run()
}

func run() {
	start := time.Now()
	var waiter sync.WaitGroup
	for i := int64(0); i < threads; i++ {
		waiter.Add(1)
		go func(idx int64) {
			defer waiter.Done()
			stress(idx)
		}(i)
	}

	waiter.Wait()
	stop := time.Now().Sub(start)
	fmt.Printf("Processed %d pushes and %d pops in %2f seconds, rate: %f jobs/s\n", pushes, pops, stop.Seconds(), float64(jobs)/stop.Seconds())
	//fmt.Println(opsCount)
}

func stress(idx int64) {
	opsCount[idx] = 0

	client, err := faktory.Open()
	if err != nil {
		handleError(err)
		return
	}
	defer client.Close()

	randomQueues := shuffle(queues)

	for {
		if idx%2 == 0 {
			push(client, randomQueue())
			newp := atomic.AddInt64(&pushes, 1)
			if newp >= jobs {
				return
			}
		} else {
			pop(client, randomQueues)
			newp := atomic.AddInt64(&pops, 1)
			if newp >= jobs {
				return
			}
		}
		opsCount[idx]++
	}
}

func randomQueue() string {
	return queues[rand.Intn(len(queues))]
}

func pop(client *faktory.Client, queues []string) {
	job, err := client.Fetch(queues...)
	if err != nil {
		handleError(err)
		return
	}
	if rand.Intn(100) == 99 {
		err = client.Fail(job.Jid, os.ErrClosed, nil)
	} else {
		err = client.Ack(job.Jid)
	}
	if err != nil {
		handleError(err)
		return
	}
}

func push(client *faktory.Client, queue string) {
	j := faktory.NewJob("SomeJob", []interface{}{1, "string", 3})
	j.Priority = uint8(rand.Intn(9) + 1)
	j.Queue = queue
	err := client.Push(j)
	if err != nil {
		handleError(err)
		return
	}
}

func handleError(err error) {
	fmt.Println(err.Error())
}

func shuffle(src []string) []string {
	dest := make([]string, len(src))
	perm := rand.Perm(len(src))
	for i, v := range perm {
		dest[v] = src[i]
	}
	return dest
}
