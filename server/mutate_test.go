package server

import (
	"testing"
	"time"

	faktory "github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestMutateCommands(t *testing.T) {
	runServer(":7419", func() {
		// launch the Faktory server
		cl, err := faktory.Open()
		assert.NoError(t, err)

		// let's clear all the jobs in `Retries` set
		// that can be pending from the previous test run
		err = cl.Clear(faktory.Retries)
		assert.NoError(t, err)

		// after the clean-up, let's verify the current state on the server,
		// we will do it numerous times down the road;
		//
		// we expect:
		//  0 jobs in "scheduled" set
		//  0 jobs in "dead" set
		//  0 jobs in "default" set
		state, err := cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 0, state.Data.Sets["dead"])
		assert.EqualValues(t, 0, state.Data.Queues["default"])

		// JOB1: schedule "AnotherJob" job to be queued in 10 seconds
		j := faktory.NewJob("AnotherJob", "truid:67123", 3)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		// JOB2: schedule "SomeJob" job to be queued in 10 seconds
		j = faktory.NewJob("SomeJob", "truid:67123", 3)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		// JOB3: schedule another "SomeJob" job to be queued in 10 seconds
		j = faktory.NewJob("SomeJob", "trid:67123", 5)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)

		// JOB4: now, schedule "FooJob" job to be queued in 10 seconds
		j = faktory.NewJob("FooJob", "445", 5)
		j.At = util.Thens(time.Now().Add(10 * time.Second))
		err = cl.Push(j)
		assert.NoError(t, err)
		targetJid := j.Jid

		// JOB5: and also schedule another "FooJob" job to be queued right away
		j = faktory.NewJob("FooJob", "445", 5)
		err = cl.Push(j)
		assert.NoError(t, err)

		// we expect 4 jobs in "scheduled" (NB: not 5 job, because the fifth job
		// was destined to go to the queue immediately)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 4, state.Data.Sets["scheduled"])

		// let's not discard jobs of type "SomeJob" matching the "*uid:67123"
		// pattern, which should target only one job out of two jobs of type "SomeJob",
		// the one with argument "truid:67123" in it (JOB2)
		err = cl.Discard(faktory.Scheduled, faktory.OfType("SomeJob").Matching("*uid:67123*"))
		assert.NoError(t, err)

		// we expect:
		//  3 jobs in "scheduled" (JOB1, JOB3, JOB4)
		//  0 jobs in "dead" set
		//  1 job in "default" set (JOB5) - the one scheduled immediately
		//  1 job thrown away (JOB2)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 3, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 0, state.Data.Sets["dead"])
		assert.EqualValues(t, 1, state.Data.Queues["default"])

		// let's kill the only job of type "AnotherJob" (JOB1 from above)
		err = cl.Kill(faktory.Scheduled, faktory.OfType("AnotherJob"))
		assert.NoError(t, err)

		// what is dead may never die (Game of Thrones, season 2, episode 3)
		err = cl.Kill("", faktory.OfType("AnotherJob"))
		assert.Error(t, err)

		// we expect:
		//  2 jobs in "scheduled" (JOB3, JOB4)
		//  1 job in "dead" set (JOB1)
		//  1 job in "default" set (JOB5)
		//  1 job thrown away (JOB2)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 2, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 1, state.Data.Sets["dead"])
		assert.EqualValues(t, 1, state.Data.Queues["default"])

		// let's now force-push JOB4
		err = cl.Requeue(faktory.Scheduled, faktory.WithJids(targetJid))
		assert.NoError(t, err)

		// we expect:
		//  1 job in "scheduled" (JOB3)
		//  1 job in "dead" set (JOB1)
		//  2 jobs in "default" set (JOB5, JOB4) - in the order they got there
		//  1 job thrown away (JOB2)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 1, state.Data.Sets["dead"])
		assert.EqualValues(t, 2, state.Data.Queues["default"])

		// let's now clear _all_ dead jobs (which we only got one  - JOB1)
		err = cl.Clear(faktory.Dead)
		assert.NoError(t, err)

		// we expect:
		//  1 job in "scheduled" (JOB3)
		//  0 jobs in "dead" set
		//  2 jobs in "default" set (JOB5, JOB4)
		//  2 jobs thrown away (JOB2, JOB1)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 0, state.Data.Sets["dead"])
		assert.EqualValues(t, 2, state.Data.Queues["default"])

		// let's now demonstrate that if they specify both `Jids` and
		// `Jobtype` when perform a mutate operation, the `Jobtype` will not
		// be taken into account by the server;
		//
		// let's kill try to kill JOB3 (which is the only job we have got
		// in the "scheduled" set, see above), specify the correct jobtype
		// for JOB3 ("SomeJob"), but not its jid, rather jids of JOB4 (which
		// by this time has already been force-pushed onto its queue)
		err = cl.Kill(faktory.Scheduled, faktory.OfType("SomeJob").WithJids(targetJid))
		assert.NoError(t, err)

		// we expect NO CHANGES since last check-point:
		//  1 job in "scheduled" (JOB3)
		//  0 jobs in "dead" set
		//  2 jobs in "default" set (JOB5, JOB4)
		//  2 jobs thrown away (JOB2, JOB1)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 0, state.Data.Sets["dead"])
		assert.EqualValues(t, 2, state.Data.Queues["default"])

		// now let's repeat the previous mutation but WITHOUT specifying jids
		err = cl.Kill(faktory.Scheduled, faktory.OfType("SomeJob"))
		assert.NoError(t, err)

		// we expect JOB3 to travel from "scheduled" to "dead", and so:
		//  0 jobs in "scheduled"
		//  1 job in "dead" set (JOB3)
		//  2 jobs in "default" set (JOB5, JOB4)
		//  2 jobs thrown away (JOB2, JOB1)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 1, state.Data.Sets["dead"])
		assert.EqualValues(t, 2, state.Data.Queues["default"])

		// finally, let's demonstrate that if they specify both `Jids` and
		// `Matching` when perform a mutate operation, the `Matching` pattern
		// will not be taken into account by the server (similar to the `Jobtype`
		// case we described above)
		//
		// we've got JOB3 in the "dead" set, let's try to re-queue it specifying
		// its kind ("SomeJob"), but not its jid (similar how we did before
		// in this test, using id of JOB4)
		err = cl.Requeue(faktory.Dead, faktory.OfType("SomeJob").WithJids(targetJid))
		assert.NoError(t, err)

		// we expect NO CHANGES since last check-point:
		//  0 jobs in "scheduled" set
		//  1 job in "dead" set (JOB3)
		//  2 jobs in "default" set (JOB5, JOB4)
		//  2 jobs thrown away (JOB2, JOB1)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 1, state.Data.Sets["dead"])
		assert.EqualValues(t, 2, state.Data.Queues["default"])

		// let's now repeat the `Requeue` mutation, but without giving `Jid`s
		// in the filter:
		err = cl.Requeue(faktory.Dead, faktory.OfType("SomeJob"))
		assert.NoError(t, err)

		// we expect JOB3 to have been moved from "dead" to "default", i.e.:
		//  0 jobs in "scheduled" set
		//  0 jobs in "dead" set
		//  3 jobs in "default" set (JOB5, JOB4, JOB3)
		//  2 jobs thrown away (JOB2, JOB1)
		state, err = cl.CurrentState()
		assert.NoError(t, err)
		assert.EqualValues(t, 0, state.Data.Sets["scheduled"])
		assert.EqualValues(t, 0, state.Data.Sets["dead"])
		assert.EqualValues(t, 3, state.Data.Queues["default"])

	})
}
