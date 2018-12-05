package manager

import (
	"context"
	"fmt"

	"github.com/contribsys/faktory/client"
)

type MiddlewareFunc func(next func() error, ctx Context) error
type MiddlewareChain []MiddlewareFunc

type Context interface {
	context.Context

	Job() *client.Job
	Manager() Manager
}

type Ctx struct {
	context.Context

	job *client.Job
	mgr *manager
}

func (c Ctx) Job() *client.Job {
	return c.job
}

func (c Ctx) Manager() Manager {
	return c.mgr
}

func Halt(msg string) error {
	return halt{msg: msg}
}

type halt struct {
	msg string
}

func (h halt) Error() string {
	return fmt.Sprintf("Halt: %s", h.msg)
}

// Run the given job through the given middleware chain.
// `final` is the function called if the entire chain passes the job along.
func callMiddleware(chain MiddlewareChain, ctx Context, final func() error) error {
	if len(chain) == 0 {
		return final()
	}

	link := chain[0]
	rest := chain[1:]
	return link(func() error { return callMiddleware(rest, ctx, final) }, ctx)
}
