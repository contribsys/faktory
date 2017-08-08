# Faktory

At a high level, Faktory is the central repository for background jobs
within your application. Jobs have a name and a set of
arguments and placed into queues.

You can use this server to distribute jobs to one or hundreds of
machines.  Jobs can be executed with any language by clients using
the Faktory API to fetch a job from a queue.

More specifically, this is a Unix server daemon which provides
language-agnostic, persistent background jobs.

## Upcoming Features

- Jobs are represented as simple JSON hashes.
- Jobs are pushed and pulled from queues.
- Jobs are reserved by clients with a timeout.
- Errors within a job trigger a retry workflow with exponential backoff.
- Exposes basic usage metrics.
- Contains a comprehensive Web UI for management and monitoring.

## API

Under development.

## License

Worq is licensed GPLv3.

## Author

Mike Perham, @mperham and other contributors.
