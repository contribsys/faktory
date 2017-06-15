package worq

import (
	"container/list"
	"sync"
)

type Queue struct {
	Name      string
	contents  *list.List
	listeners *list.List
}

var queues = map[string]*Queue{}
var queueLock = sync.Mutex{}

func LookupQueue(name string) *Queue {
	queueLock.Lock()
	defer queueLock.Unlock()

	_, ok := queues[name]
	if !ok {
		queues[name] = NewQueue(name)
	}
	return queues[name]
}

func NewQueue(name string) *Queue {
	return &Queue{Name: name, contents: list.New(), listeners: list.New()}
}

func (q *Queue) Push(jobs ...*Job) error {
	for _, job := range jobs {
		q.contents.PushBack(job)
	}
	return nil
}

func (q *Queue) Size() int {
	return q.contents.Len()
}

func (q *Queue) Pop() *Job {
	if q.contents.Len() == 0 {
		return nil
	}

	return q.contents.Remove(q.contents.Front()).(*Job)
}
