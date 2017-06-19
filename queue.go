package storage

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

func (db *Store) LookupQueue(name string) *Queue {
	queueLock.Lock()
	defer queueLock.Unlock()

	_, ok := queues[name]
	if !ok {
		queues[name] = db.NewQueue(name)
	}
	return queues[name]
}

func (db *Store) NewQueue(name string) *Queue {
	return &Queue{Name: name, contents: list.New(), listeners: list.New()}
}

func (db *Store) Pop(names ...string) interface{} {
	for _, qname := range names {
		j := db.LookupQueue(qname).Pop()
		if j != nil {
			return j
		}
	}
	return nil
}

func (q *Queue) Push(jobs ...interface{}) error {
	for _, job := range jobs {
		q.contents.PushBack(job)
	}
	return nil
}

func (q *Queue) Size() int {
	return q.contents.Len()
}

func (q *Queue) Pop() interface{} {
	if q.contents.Len() == 0 {
		return nil
	}

	return q.contents.Remove(q.contents.Front())
}
