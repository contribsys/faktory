package worq

import "time"

type Client interface {
	Identity() string
	Close()
	Get(queues ...string) (*Job, error)
	Ack(jid string) error
	Fail(jid string, error_message string, error_class string, ctx map[string]interface{}) error
}

// These hold our scheduled jobs and pending retries
type SortedSet interface {
	AddElement(when time.Time, jid string, payload []byte) error
	RemoveBefore(when time.Time) ([][]byte, error)
	Size() int
}
