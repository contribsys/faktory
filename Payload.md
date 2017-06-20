## Job Payload

Worq's job payload format is slightly different from Sidekiq's, in order to
better fit different types of languages and solve a few implementation difficulties.

The payload format is JSON, since it is the only data format widely
supported in the standard library of all modern languages.  Polyglot
support is one of the core features of Worq.

For datatypes, we stick with Strings and Integers and avoid Floats and Booleans.[^1]

```
{
  #################################
  # required fields
  #################################

  # the unique ID for this job, can be any random unique
  # string, default: 12 random bytes, base64urlsafe-encoded
  "jid": "some_string",

  # the identifier for this type of job, was "class" in Sidekiq but
  # that term isn't friendly to non-OOP languages and is often a
  # keyword.  This is an arbitrary string that the executor can use to
  # dispatch the job to the right code for execution.
  "jobtype": "SomeWorker",

  # the arguments to the job
  "args": [1,2,3],

  # the queue for the job, /[a-zA-Z][a-zA-Z0-9_-]*/
  "queue": "default",

  #################################
  # everything below here is OPTIONAL
  #################################

  # Timestamps associated with this job.
  #
  # Sidekiq stores all timestamps as epoch floats but floats
  # are lossy and problematic in JSON.
  #
  # Precision demands a string form so we use RFC3339.
  # Timestamp strings MUST be exactly 27 characters long, including
  # any trailing zeros for the microseconds, and in UTC.
  #
  # Please note that this format is not Y10K compliant and will need
  # significant redesign in less than 8000 years.
  #
  # Worq will automatically set this field if you do not
  # provide it.
  "created_at": "2006-01-02T15:04:05.001000Z",

  # Worq will automatically set this field
  "enqueued_at": "2006-01-02T15:04:05.001001Z",

  # number of seconds to reserve this job for, default 600
  # if the job is not acknowledged within this time, the job will be
  # re-queued for execution.
	"reserve_for": 30,

  #######################################
  # Failures and the Retry subsystem
  #######################################

  # in case of failure, the number of times to retry the job.
  # the retry subsystem uses an exponential backoff algorithm
  # to automatically recover in case of, e.g. common network failures.
  #
  # setting retry to 0 means job failure will move the job immediately
  # to the Dead set.
  #
  # setting retry to -1 means the job is considered transient and discarded upon failure
  "retry": 25,

  # enable backtrace collection and storage for failues, where the value
  # is the number of stack frames to collect.
  # this defaults to 0 because it greatly expands the amount of
  # space required for storing retries.
  "backtrace": 50,

  # this sub-structure stores the current failure data
  "failure": {

    # timestamp of the first failure for this job
    "failed_at": "2006-01-02T15:04:05.001001Z",

    # how many times it has failed so far
    "retry_count": 7,

    # type and message of the failure cause
    "message": "No such thing",
    "errtype": "ArgumentError",

    # backtrace for the last retry failure, array of strings
    "backtrace": [
      "lib/something.rb:123",
      "myapp.rb:15",
    ]
  },

  ###############################
  # Custom Attributes
  ##############################

  # Jobs may have a bag of arbitrary attributes that enable additional
  # functionality. Worq will not touch this data but pass it on unchanged.
  "custom": {
    "locale": "EN_us",
    "tz": "US/Chicago",
    "account_id": 123456789,
  }

  #####################################
  # WARNING
  # Any unknown keys within the job hash that aren't in the
  # `custom` hash will be discarded by Worq.
  #####################################
}
```

[^1]: Someone once told me that booleans always expand to three values and
that jibes with my experience too.  A boolean value like `registered` is more
useful as a timestamp `registered_at`.  Even gender has come to include
M/F/Non-binary as accepted values.  Booleans: not even once.
