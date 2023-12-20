package webui

import (
	"testing"

	"github.com/contribsys/faktory/client"
)

type testJob struct {
	name     string
	job      *client.Job
	expected string
}

var testCases = []testJob{
	{
		name:     "simple",
		job:      client.NewJob("foo_job", 1),
		expected: "foo_job",
	},
	{
		name:     "wrapped",
		job:      client.NewJob("ActiveJob::QueueAdapters::FaktoryAdapter::JobWrapper", "[{job_class: 'FooJob', args: [1]}]"),
		expected: "FooJob",
	},
}

func TestActiveJobUnwrapping(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := displayJobType(tc.job)
			if tc.expected != result {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}
