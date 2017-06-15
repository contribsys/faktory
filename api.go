package worq

type Client interface {
	Identity() string
	Close()
	Get(queues ...string) (*Job, error)
	Ack(jid string) error
	Fail(jid string, error_message string, error_class string, ctx map[string]interface{}) error
}
