package manager

import (
	"github.com/contribsys/faktory/client"
)

type MiddlewareFunc func(next func() error, job *client.Job) error
type MiddlewareChain []MiddlewareFunc

// Run the given job through the given middleware chain.
// `final` is the function called if the entire chain passes the job along.
func callMiddleware(chain MiddlewareChain, job *client.Job, final func() error) error {
	if len(chain) == 0 {
		return final()
	}

	link := chain[0]
	rest := chain[1:]
	return link(func() error { return callMiddleware(rest, job, final) }, job)
}
