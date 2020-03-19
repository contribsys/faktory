package client

import (
	"encoding/json"
)

//
// Faktory's Mutate API allows clients to directly mutate the various
// persistent sets within Faktory. These use cases are typically for
// repair or data migration purposes.
//
// THESE APIs SHOULD NEVER BE USED WITHIN APP LOGIC.
// Many Mutate API use cases will have poor performance:
// O(N), O(N log N), or even O(M*N).

type Structure string

type JobFilter struct {
	Jids    []string `json:"jids,omitempty"`
	Regexp  string   `json:"regexp,omitempty"`
	Jobtype string   `json:"jobtype,omitempty"`
}

func (jf JobFilter) WithJids(jids ...string) JobFilter {
	jf.Jids = jids
	return jf
}

func (jf JobFilter) Matching(pattern string) JobFilter {
	jf.Regexp = pattern
	return jf
}

func (jf JobFilter) OfType(jobtype string) JobFilter {
	jf.Jobtype = jobtype
	return jf
}

const (
	Scheduled Structure = "scheduled"
	Retries   Structure = "retries"
	Dead      Structure = "dead"
)

var (
	Everything = JobFilter{
		Regexp: "*",
	}
)

// Match jobs with the given JIDs. Warning: O(m*n), very slow
// because it has to pull every job into Faktory and check the JID
// against the list.
//
// If you pass in a single JID, it will devolve to matching within Redis
// and perform much faster.  For that reason, it might be better to
// handle one JID at a time.
func WithJids(jids ...string) JobFilter {
	return JobFilter{
		Jids: jids,
	}
}

// This is a generic pattern match across the entire job JSON payload.
// Be very careful that you don't accidentally match some unintended part
// of the payload.
//
// NB: your pattern should have * on each side.  The pattern is passed
// directly to Redis.
//
// Example: discard any job retries whose payload contains the special word "uid:12345":
//
//     client.Discard(faktory.Retries, faktory.Matching("*uid:12345*"))
//
// See the Redis SCAN documentation for pattern matching examples.
// https://redis.io/commands/scan
func Matching(pattern string) JobFilter {
	return JobFilter{
		Regexp: pattern,
	}
}

// Matches jobs based on the exact Jobtype. This is pretty fast because
// it devolves to Matching(`"jobtype":"$ARG"`) and matches within Redis.
func OfType(jobtype string) JobFilter {
	return JobFilter{
		Jobtype: jobtype,
	}
}

type Operation struct {
	Cmd    string     `json:"cmd"`
	Target Structure  `json:"target"`
	Filter *JobFilter `json:"filter,omitempty"`
}

// Commands which allow you to perform admin tasks on various Faktory structures.
// These are NOT designed to be used in business logic but rather for maintenance,
// data repair, migration, etc.  They can have poor scalability or performance edge
// cases.
//
// Generally these operations are O(n) or worse.  They will get slower as your
// data gets bigger.
type MutateClient interface {

	// Move the given jobs from structure to the Dead set.
	// Faktory will not touch them anymore but you can still see them in the Web UI.
	//
	// Kill(Retries, OfType("DataSyncJob").WithJids("abc", "123"))
	Kill(name Structure, filter JobFilter) error

	// Move the given jobs to their associated queue so they can be immediately
	// picked up and processed.
	Requeue(name Structure, filter JobFilter) error

	// Throw away the given jobs, e.g. if you want to delete all jobs named "QuickbooksSyncJob"
	//
	//   Discard(Dead, OfType("QuickbooksSyncJob"))
	Discard(name Structure, filter JobFilter) error

	// Empty the entire given structure, e.g. if you want to clear all retries.
	// This is very fast as it is special cased by Faktory.
	Clear(name Structure) error
}

func (c *Client) Kill(name Structure, filter JobFilter) error {
	return c.mutate(Operation{Cmd: "kill", Target: name, Filter: &filter})
}

func (c *Client) Requeue(name Structure, filter JobFilter) error {
	return c.mutate(Operation{Cmd: "requeue", Target: name, Filter: &filter})
}

func (c *Client) Discard(name Structure, filter JobFilter) error {
	return c.mutate(Operation{Cmd: "discard", Target: name, Filter: &filter})
}

func (c *Client) Clear(name Structure) error {
	return c.mutate(Operation{Cmd: "clear", Target: name, Filter: nil})
}

func (c *Client) mutate(op Operation) error {
	j, err := json.Marshal(op)
	if err != nil {
		return err
	}
	err = writeLine(c.wtr, "MUTATE", j)
	if err != nil {
		return err
	}

	return ok(c.rdr)
}
