# Faktory API

The Faktory API encompasses the few specific operations necessary to
enqueue and dequeue background jobs.  The **client** enqueues a job to
the **server**.  The **worker** dequeues a job from the **server**.

Jobs are a simple map of key/values in JSON format:

```
{
  "jid":"981237123987abcf",
  "queue":"default",
  "jobtype":"SomeWorker",
  "args":["123", 456]
}
```

## Requests

Requests are line-oriented, with the form:

<VERB> <params>\\r\\n

The \\r\\n line ending is mandatory.  By default, Faktory listens at
localhost:7419.

## AHOY

AHOY k:v k:v

The AHOY command is required to establish a connection with the Faktory server.

The only mandatory key/value is "pwd:somevalue" when not listening
on localhost.  The server password is randomly generated upon
installation.  Best practice is a random 20+ character hex string, e.g.

> ruby -rsecurerandom -e 'puts SecureRandom.hex(10)'

In development mode, Faktory only listens on localhost and so does not
require a password.

## PUSH

PUSH <json>
=> OK

Pushes the job into the given queue.  Each job must have a unique JID.

## RESERVE

RESERVE <queue1> <queue2> <queue3>
=> [A bulk string with the json blob]

Grab a job from the first non-empty queue.

## ACK

ACK <jid>
=> OK

Once a job has been reserved by a worker, use ACK to signal the job has been processed and can be deleted.

## FAIL

FAIL <jid> <json>
=> OK

Signal that <jid> processing has failed, this triggers the retry
process.  <json> is an optional hash of data about the failure:

```
{
  "errmsg":"Invalid arguments",
  "backtrace":[
    "lib/foo.rb:45",
    "lib/foo.rb:13",
    "lib/foo.rb:78"
  ]
}
```


## END

END
=> OK

Closes the server connection.


## Responses

Faktory uses the [Redis Serialization Protocol](https://redis.io/topics/protocol) to send responses.
The default response is "+OK\r\n" which corresponds to "OK".  All
commands that return "OK" can also return errors.

