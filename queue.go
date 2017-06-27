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

var (
	queues    = map[string]*Queue{}
	queueLock = sync.Mutex{}
)

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

func Pop(f func(*Job) error, names ...string) (*Job, error) {
	for _, qname := range names {
		q := LookupQueue(qname)
		j := q.Pop()
		if j != nil {
			err := f(j)
			if err != nil {
				return nil, err
			}
			return j, nil
		}
	}
	// TODO Blocking
	return nil, nil
}

func (q *Queue) Push(jobs ...*Job) error {
	queueLock.Lock()
	defer queueLock.Unlock()

	for _, job := range jobs {
		q.contents.PushBack(job)
	}
	return nil
}

func (q *Queue) Size() int {
	return q.contents.Len()
}

func (q *Queue) Pop() *Job {
	queueLock.Lock()
	defer queueLock.Unlock()

	if q.contents.Len() == 0 {
		return nil
	}

	return q.contents.Remove(q.contents.Front()).(*Job)
}
