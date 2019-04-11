package manager

import (
	"context"

	"github.com/contribsys/faktory/client"
)

type MiddlewareFunc func(next func() error, ctx Context) error
type MiddlewareChain []MiddlewareFunc

type Context interface {
	context.Context

	Job() *client.Job
	Manager() Manager
	Reservation() *Reservation
}

type Ctx struct {
	context.Context

	job *client.Job
	mgr *manager
	res *Reservation
}

func (c Ctx) Reservation() *Reservation {
	return c.res
}

func (c Ctx) Job() *client.Job {
	return c.job
}

func (c Ctx) Manager() Manager {
	return c.mgr
}

// Returning a Halt error in a middleware will stop the middleware execution
// chain.  The server will return the Halt to the client.  You can use "ERR"
// for the code to signal an unexpected error or use a well-defined code for
// an error case which the client might be interested in, e.g. "NOTUNIQUE".
func Halt(code string, msg string) error {
	return ExpectedError(code, msg)
}

// Middleware can use this to restart the fetch process.  Useful if the job
// fetched from Redis is invalid and should be discarded rather than returned
// to the worker.
func Discard(msg string) error {
	return ExpectedError("DISCARD", msg)
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
