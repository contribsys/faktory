Faktory Work Protocol
---------------------

The Factory Work Protocol (FWP) allows a client interact with a Faktory
work server. It permits a client to authenticate to a Faktory server,
submit units of work for later execution, and/or fetch units of work for
processing and subsequently report their execution result.

FWP does not dictate how work units are scheduled or executed, nor their
semantics. This is left to the Faktory work server and the client
implementation respectively.

# How to Read This Document

## Organization of This Document

This document is written from the point of view of the implementor of
an FWP client.  Beyond the protocol overview in section 2, it is not
optimized for someone trying to understand the operation of the
protocol.  The material in sections 3 through 5 provides the general
context and definitions with which FWP operates.

Sections 6, 7, and 9 describe the FWP commands, responses, and syntax,
respectively.  The relationships among these are such that it is almost
impossible to understand any of them separately.  In particular, do not
attempt to deduce command syntax from the command section alone; instead
refer to the Formal Syntax section.

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
| `priority`    | Integer [1-9]  | 5              | higher priority jobs are scheduled before lower priority jobs.
| `reserve_for` | Integer [60+]  | 1800           | number of seconds a job may be held by a worker before it is considered failed.
| `at`          | RFC3339 string | \<blank\>      | run the job at approximately this time; immediately if blank
| `retry`       | Integer        | 25             | number of times to retry this job if it fails. -1 prevents retries.
| `backtrace`   | Integer        | 0              | number of lines of FAIL information to preserve.
| `created_at`  | RFC3339 string | set by server  | used to indicate the creation time of this job.
| `custom`      | JSON hash      | `null`         | provides additional context to the worker executing the job.

### Read-only fields for enqueued jobs

| Field name    | Value type     | Description |
| ------------- | -------------- | ----------- |
| `enqueued_at` | RFC3339 string | the time at which this job was enqueued by the server.
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

# Client States

Once the connection between client and server is established, an FWP
connection is in one of several states; initially "Not identified". Most
commands are only valid in the "Identified" state. It is a protocol
error for the client to attempt a command while the connection is in an
inappropriate state, and the server will respond with an error.

Clients that wish to act as consumers, that is, to execute jobs, MUST
regularly issue `BEAT` commands to the server to recognize state changes
initiated by the server. Clients that are not consumers MUST NOT send
`BEAT` commands, and MUST NOT enter the Quiet or Terminating stages.

## Not Identified State

In the not identified state, the client MUST issue a `HELLO` command
before most other commands will be permitted. This state is entered when
a connection starts. Upon connecting, the server will first send a `HI`
message, and then wait for the client's `HELLO`.

### Identified State

This state is entered as a result of a successful client `HELLO`. In
this state, the client can issue commands at will.

### Quiet State

This state is entered as a result of a Quiet server response to a
consumer `BEAT`. In the quiet state, a consumer SHOULD NOT fetch further
jobs for execution. While in this state, the consumer MUST NOT
terminate.

### Terminating State

This state is entered as a result of a Terminate server response to a
consumer `BEAT`. In the terminating state, a consumer SHOULD NOT fetch
further jobs for execution, and SHOULD issue a `FAIL` for any currently
executing jobs. The consumer MUST then immediately enter the end state.

### End State

In the end state, the connection is being terminated.  This state can be
entered as a result of a client request (via the `END` command) or by
unilateral action on the part of either the client or server.

If the client requests the end state, the server MUST send an `OK`
response to the `END` command before the server closes the connection;
and the client MUST read the `OK` response to the `END` command before
the client closes the connection.

A server MUST NOT unilaterally close the connection to a consumer
without sending a Quiet or Terminating response to a `BEAT` command
issued by the client. The server is allowed to unilaterally close the
connection to clients that are not consumers.

A client SHOULD NOT unilaterally close the connection, and instead
SHOULD issue an `END` command.  If the server detects that the client
has unilaterally closed the connection, the server MAY omit the `OK`
response and simply close its connection.

# Client Commands

FWP commands are described in this section.  Commands are organized by
the type of client likely to issue them (producer first, then consumer). 

Command arguments, identified by "Arguments:" in the command
descriptions below, are described by function, not by syntax.  The
precise syntax of command arguments is described in the Formal Syntax
section.

All commands cause specific server responses to be returned; these are
identified by "Responses:" in the command descriptions below. See the
response descriptions in the Responses section for information on these
responses, and the Formal Syntax section for the precise syntax of these
responses.

The state of a connection is only changed by successful commands which
are documented as changing state. A rejected command (error response)
never changes the state of the connection.

## Client Commands

### `HELLO` Command

Arguments: JSON hash of client information

Responses:

 - String "OK" - client connection accepted
 - Error - client connection declined

The `HELLO` command MUST be the first command issued by any client when
connecting to a Faktory server. It is sent in response to the server's
initial `HI` message.

### `INFO` Command

### `END` Command

## Producer Commands

### `PUSH` Command

## Consumer Commands

### `FETCH` Command

### `ACK` Command

### `FAIL` Command
