# Worq

At a high level, Worq is the central coordinator for units of work
within your application. Jobs have a name and a set of
arguments -- they can be implemented in any language -- and placed into
queues.

You can use this server to distribute jobs to one or hundreds of
machines.  This project is written in Go but jobs can be executed
with any language by clients using the API to fetch a job from a queue.

More specifically, this is a Unix server daemon which provides
language-agnostic, persistent background jobs.

## Upcoming Features

- Jobs are represented as simple hashes.
- Jobs are pushed and pulled from queues.
- Job reservation with timeout.
- Errors within a job trigger the retry workflow.

## API

Under development.

## License

Worq is licensed GPLv3.

## Author

Mike Perham, @mperham and other contributors.
