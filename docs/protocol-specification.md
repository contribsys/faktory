Faktory Work Protocol
---------------------

The Factory Work Protocol (FWP) allows a client to interact with a
Faktory work server. It permits a client to authenticate to a Faktory
server, submit units of work for later execution, and/or fetch units of
work for processing and subsequently report their execution result.

FWP does not dictate how work units are scheduled or executed, nor their
semantics. This is left to the Faktory work server and the client
implementation respectively.

# How to Read This Document

## Organization of This Document

This document is written from the point of view of the implementor of
an FWP client.  Beyond the protocol overview in section 2, it is not
optimized for someone trying to understand the operation of the
protocol.  The material in sections 3 and 4 provides the general context
and definitions with which FWP operates. Sections 5 describes the FWP
commands, responses, and syntax.

## Conventions Used in This Document

"Conventions" are basic principles or procedures.  Document conventions
are noted in this section.

In examples, "C:" and "S:" indicate lines sent by the client and server
respectively.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "MAY", and "OPTIONAL" in this document are to
be interpreted as described in
[RFC2119](https://tools.ietf.org/html/rfc2119).

The word "can" (not "may") is used to refer to a possible circumstance
or situation, as opposed to an optional facility of the protocol.

"User" is used to refer to a human user, whereas "client" refers to
the software being run by the user.

"Connection" refers to the entire sequence of client/server interaction
from the initial establishment of the network connection until its
termination.

"Session" refers to the sequence of client/server interaction from
the time that a client authenticates (`HELLO` command) until
the time that the client leaves (`END` command or connection
termination).

"Work unit" refers to a single job that can be scheduled by the Faktory
work server, and that can be executed by clients. "Job" is used as a
synonym for work unit throughout this document.

"Producer" refers to a client that issues new work units to the server.
"Consumer" refers to a client that fetches work units from the server
for execution. "Worker" is used as a synonym for "Consumer" throughout
this document. A single client can act as both a consumer and a producer.

All text is encoded in UTF-8 unless otherwise specified.

# Protocol Overview

## Link Level

The FWP protocol assumes a reliable data stream such as that provided by
TCP. When TCP is used, an FWP server listens on port 7419.

## Commands and Responses

An FWP connection consists of the establishment of a client/server
network connection, an initial greeting from the server, a greeting
response from the client, and any number of client/server interactions.
These client/server interactions consist of a single client command,
followed by a single server response.

### Client Protocol Sender and Server Protocol Receiver

The client command begins an operation. Operations are untagged, and
only one command may be executed at a time on a single connection.

All commands transmitted by clients are in the form of lines, that is,
strings that end with CRLF. Every command consists of a *verb*,
optionally followed by a space and any number of arguments. A CRLF
terminates the command.

Clients MUST follow the syntax outlined in this specification strictly.
It is a syntax error to send a command with missing or extraneous spaces
or arguments.

The protocol receiver of an FWP server reads a command line from the
client, parses the command and its arguments, and transmits the server
data response.

### Server Protocol Sender and Client Protocol Receiver

With the exception of the initial `HI` greeting from the server, the
server MUST send data only as a result of a client command.

All responses sent by a FWP server are encoded using redis
[RESP](https://redis.io/topics/protocol); a simple text-based yet binary
safe protocol that is simple and efficient to parse. See the RESP
specification linked above for details.

Whenever a client command results in a failure, the returned server data
MUST be encoded as a RESP
[Error](https://redis.io/topics/protocol#resp-errors).

Servers SHOULD enforce the syntax outlined in this specification
strictly.  Any client command with a protocol syntax error, including
(but not limited to) missing or extraneous spaces or arguments, SHOULD
be rejected, and the client given a server error response.

# Work units

Work units are the principal unit used by producers and consumers in
FWP. They are always encoded as JSON dictionaries, and contain different
fields depending on the situation.

### Required fields

| Field name | Value type | Description |
| ---------- | ---------- | ----------- |
| `jid`      | String     | globally unique ID for the job.
| `jobtype`  | String     | discriminator used by a worker to decide how to execute a job.
| `args`     | Array      | parameters the worker should use when executing the job.

### Optional fields

| Field name    | Value type     | When omitted   | Description |
| ------------- | -------------- | -------------- | ----------- |
| `queue`       | String         | `default`      | which job queue to push this job onto.
| `reserve_for` | Integer [60+]  | 1800           | number of seconds a job may be held by a worker before it is considered failed.
| `at`          | RFC3339 string | \<blank\>      | run the job at approximately this time; immediately if blank
| `retry`       | Integer        | 25             | number of times to retry this job if it fails. 0 discards the failed job, -1 saves the failed job to the dead set.
| `backtrace`   | Integer        | 0              | number of lines of FAIL information to preserve.
| `created_at`  | RFC3339 string | set by server  | used to indicate the creation time of this job.
| `custom`      | JSON hash      | `null`         | provides additional context to the worker executing the job.

### Read-only fields for enqueued jobs

| Field name    | Value type     | Description |
| ------------- | -------------- | ----------- |
| `enqueued_at` | RFC3339 string | the most recent time this job was enqueued by the server.
| `failure`     | JSON hash      | data about this job's most recent failure (if any).

### Work unit state diagram

When the server is given a new work unit, the work unit starts out as
`SCHEDULED`. After its `at` time expires, or immediately if `at` is
unset, the work unit enters the `ENQUEUED` state, where it can be
returned to consumers that request work using `FETCH`.

Once a work unit has been fetched, it enters the `WORKING` state. It
remains in this state either until the responsible consumer sends an
explicit `ACK` or `FAIL` for that work unit, or until the unit's
reservation timer (`reserve_for`) expires. If an `ACK` is sent for the
work unit, it is purged from the server. If a `FAIL` is sent, or its
reservation expires, the work unit enters the `RETRIES` state.

If a retrying job has reached its `retry` limit, it is killed, and
marked as `DEAD`. Otherwise, it is eventually enqueued again so that
another worker can complete it.

This lifecycle is represented through the following state diagram:

```
PUSH (at)  
------------> [SCHEDULED]
                ||   |                     
                ||   | (time passes)       
                ||   |                     
PUSH            ||   v         FETCH                 ACK
-----------------> [ENQUEUED] ---------> [WORKING] --------> *poof*
                ||   ^    ^______________'  |
                ||   |      reservation     |
                ||   |        expires       |
                ||   |                      |
                ||   |                      |
                ||   | (time passes)        |
                ||   |                      |
                ||   |       FAIL           |
                 [RETRIES] <---------------'
                ||   |
                ||   |  retries exhausted/kill (web ui only)
                ``    `--------------------------------> 
                 ``    kill (web ui only)                [DEAD]
                  ``===================================> 
```

# Client Lifecycle

Once the connection between client and server is established, an FWP
connection is in one of several states; initially "Not identified". Most
commands are only valid in the "Identified" state. It is a protocol
error for the client to attempt a command while the connection is in an
inappropriate state, and the server will respond with an error.

Clients that wish to act as consumers, that is, to execute jobs, MUST
regularly issue `BEAT` commands to the server to recognize state changes
initiated by the server. Clients SHOULD NOT send these more frequently
than every 5 seconds, and MUST send them at least every 60 seconds. A
`BEAT` interval of 15 seconds is recommended. Clients that are not
consumers MUST NOT send `BEAT` commands, and MUST NOT enter the Quiet or
Terminating stages.

## Not Identified State

In the not identified state, the client MUST issue a `HELLO` command
before most other commands will be permitted. This state is entered when
a connection starts. Upon connecting, the server will first send a `HI`
message, and then wait for the client's `HELLO`. The server `HI` is sent
as a single-line Simple String response, starting with the three-byte
sequence `HI `, followed by a single JSON hash with the following
fields:

| Field name | Value type | Description |
| ---------- | ---------- | ----------- |
| `v`        | Integer    | protocol version number. always 2 for servers conforming to this FWP specification.
| `i`        | Integer    | only present when password is required. number of password hash iterations. see `HELLO`.
| `s`        | String     | only present when password is required. salt for password hashing. see `HELLO`.

### Identified State

This state is entered as a result of a successful client `HELLO`. In
this state, the client can issue commands at will.

### Quiet State

This state is entered as a result of a Quiet server response to a
consumer `BEAT`. In the quiet state, a consumer SHOULD NOT fetch further
jobs for execution. While in this state, the consumer MUST NOT
terminate, and MUST continue to issue regular `BEAT` commands.

### Terminating State

This state is entered as a result of a Terminate server response to a
consumer `BEAT`. In the terminating state, a consumer MUST NOT fetch
further jobs for execution, and SHOULD issue a `FAIL` for any currently
executing jobs within 30 seconds. The consumer MUST enter the end state
after at most 30 seconds.

### End State

In the end state, the connection is being terminated.  This state can be
entered as a result of a client request (via the `END` command) or by
unilateral action on the part of either the client or server.

If the client requests the end state, the server will close the
connection immediately. There is no response to read; the client closes the
connection immediately after requesting the end state.

A server MUST NOT unilaterally close the connection to a consumer
without sending a Quiet or Terminating response to a `BEAT` command
issued by the client. The server is allowed to unilaterally close the
connection to clients that are not consumers.

A client SHOULD NOT unilaterally close the connection, and instead
SHOULD issue an `END` command.

# Client Commands

FWP commands are described in this section.  Commands are organized by
the type of client likely to issue them (producer first, then consumer).

Command arguments, identified by "Arguments:" in the command
descriptions below, are shown either a metavariables (e.g., "queue"), or
as JSON hash templates. The precise meaning is defined in the body text.

All commands cause specific server responses to be returned; these are
identified by "Responses:" in the command descriptions below.

The state of a connection is only changed by successful commands which
are documented as changing state. A rejected command (error response)
never changes the state of the connection.

## Client Commands

### `HELLO` Command

Arguments: JSON hash of client information

Responses:

 - Simple String "OK" - client connection accepted
 - Error - client connection declined

The `HELLO` command MUST be the first command issued by any client when
connecting to a Faktory server. It is sent in response to the server's
initial `HI` message.

A `HELLO` contains identifying information about the client, as well as
authentication credentials if the server requests them. Clients MUST
supply the following fields:

| Field name | Value type | Description |
| ---------- | ---------- | ----------- |
| `v`        | Integer    | protocol version number. always 2 for clients conforming to this FWP specification.

In response to a client `HELLO`, the server will send either a
Simple String OK response, or an error. If an OK response is received,
the client enters the "Identified" state, and may begin issuing other
commands.

#### Required Fields for Protected Server

When the server `HI` includes an iteration count `i` and a salt `s`,
a client MUST include a `pwdhash` String-typed field in their `HELLO`.
This field should be the hexadecimal representation of the `i`th SHA256
hash of the client password concatenated with the value in `s`.

```example
hash = password + s
for 0..i {
  hash = sha256(hash)
}
hex(hash)
```

#### Required Fields for Consumers

A client that wishes to act as a consumer MUST include the following
additional fields in their `HELLO`:

| Field name | Value type    | Description |
| ---------- | ------------- | ----------- |
| `hostname` | String        | name for the host that is running this worker.
| `wid`      | String        | globally unique identifier for this worker.
| `pid`      | Integer       | local process identifier for this worker on its host.
| `labels`   | Array[String] | labels that apply to this worker, to allow producers to target work units to worker types.

A client is allowed to establish multiple connections to the server, and
use the same `wid` value across connections. If this is done, the same
`hostname`, `pid`, and `labels` values MUST be provided in all the
`HELLO` commands for those connections.

#### Examples

Producer connecting to non-secured server:

```example
S: +HI {"v":2}
C: HELLO {"v":2}
S: +OK
```

Consumer connecting to non-secured server:

```example
S: +HI {"v":2}
C: HELLO {"hostname":"localhost","wid":"4qpc2443vpvai","pid":2676,"labels":["golang"],"v":2}
S: +OK
```

Producer connecting to a protected server:

```example
S: +HI {"v":2,"s":"123456789abc","i":1735}
C: HELLO {"pwdhash":"1e440e3f3d2db545e9129bb4b63121b6b09d594dae4344d1d2a309af0e2acac1","v":2}
S: +OK
```

### `FLUSH` Command

Arguments: *none*

Responses:

 - Simple String "OK" - database was cleared
 - Error - database was not cleared

`FLUSH` allows the caller to clear all info from Faktory's internal
database. It uses Redis's `FLUSHDB` command under the covers.

### `INFO` Command

Arguments: none

Responses:

 - Bulk String containing various information about the server
 - Error

### `END` Command

Arguments: *none*

Responses:

 - Simple String "OK" - client connection will be terminated

The `END` command is used by a client to signal to the server that it
wishes to terminate the connection. A client MUST NOT send additional
commands on the same connection after sending `END`. A consumer SHOULD
NOT send `END` while it has outstanding work units, instead, it should
first `FAIL` those jobs, or wait for their completion, and only
subsequently send `END`.

The server responds to an `END` with a Simple String OK response. Upon
receiving this response, the client enters the End state.

## Producer Commands

### `PUSH` Command

Arguments: work unit

Responses:

 - Simple String "OK" - work unit was enqueued
 - Error - work unit was not enqueued

`PUSH` lets producers enqueue jobs at the work server for later
execution. See the work unit specification for further details.

## Consumer Commands

### `FETCH` Command

Arguments: [queue...]

Responses:

 - Bulk String containing work unit - work unit for execution
 - Null Bulk String - no work unit available for execution
 - Error

Consumers SHOULD issue a `FETCH` command whenever they are able to
execute another work unit. `FETCH` will normally return a work unit that
has been enqueued by a producer, which the consumer should execute.

A consumer MAY include a list of queues to fetch work units from. The
server will check these queues in order, and return the first work unit
found. If no work units are found, `FETCH` will block for up to 2
seconds on the *first* queue provided. If no queue is provided, only the
`default` queue will be scanned.

If a work unit is returned from `FETCH`, the client MUST subsequently
send either an `ACK` or `FAIL` command for the `jid` of the returned
work unit. A client SHOULD send at most one `ACK` or `FAIL` for a given
job.

### `ACK` Command

Arguments: `{jid: String}`

Responses:

 - Simple String "OK" - ACK was received and accepted
 - Error - ACK was malformed or rejected

Consumers MUST issue an `ACK` command for any job it executes in
response to a `FETCH` command if its execution did not result in an
error. The argument should be a single-field JSON hash, `jid`, which
contains the `jid` included in the work unit returned by `FETCH`. This
informs the server that the job has been completed, and can be removed.

### `FAIL` Command

Arguments: `{jid: String, errtype: String, message: String, backtrace: Array[String]}`

Responses:

 - Simple String "OK" - FAIL was received and accepted
 - Error - FAIL was malformed or rejected

Consumers MUST issue a `FAIL` command for any job it executes in
response to a `FETCH` command if its execution resulted in an error.
The argument should be a JSON hash with the following fields:

| Field name  | Description |
| ----------- | ----------- |
| `jid`       | the `jid` of the job whose execution failed.
| `errtype`   | the class of error that occurred during execution.
| `message`   | a short description of the error.
| `backtrace` | a longer, multi-line backtrace of how the error occurred.

### `BEAT` Command

Arguments: `{wid: String, current_state: String, rss_kb: Integer}`

Responses:

 - Simple String "OK" - `BEAT` acknowledged.
 - Bulk String containing `{state: String}` - server-initiated state change.
 - Error - `BEAT` malformed or rejected.

Consumers MUST regularly issue the `BEAT` command to indicate liveness,
and to get notified about server-initiated state changes. The argument
to `BEAT` is a single-field JSON hash that contains the `wid` issued by
this worker in its `HELLO`.

If the Consumer receives a signal which transitions it to Quiet
or Terminate, it MAY notify Faktory by sending the `current_state`
element with either `quiet` or `terminate` as the value.

The Consumer MAY send the `rss_kb` element with the current size
of the Consumer's memory usage in Kilobytes.

If a non-OK simple string response is received, it represents a
server-initiated state change. The `state` field of the returned JSON
hash contains one of two values: "quiet" or "terminate". The client MUST
immediately enter the associated lifecycle state upon receiving either
of these messages.

#### Examples

```example
C: BEAT {"wid": "4qpc2443vpvai","rss_kb":54176}
S: +OK
C: BEAT {"wid": "4qpc2443vpvai","rss_kb":55272}
S: +{"state": "quiet"}
C: BEAT {"wid": "4qpc2443vpvai","current_state": "quiet"}
S: +{"state": "terminate"}
C: END
S: +OK
```
