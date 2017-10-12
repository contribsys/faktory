# Faktory

At a high level, Faktory is the central repository for background jobs
within your application. Jobs have a name and a set of
arguments and placed into queues.

You can use this server to distribute jobs to one or hundreds of
machines.  Jobs can be executed with any language by clients using
the Faktory API to fetch a job from a queue.

More specifically, this is a Unix server daemon which provides
language-agnostic, persistent background jobs.

## Basic Features

- Jobs are represented as simple JSON hashes.
- Jobs are pushed and fetched from queues.
- Jobs are reserved with a timeout, 30 min by default.
- Jobs not ACK'd or FAIL'd within timeout are requeued.
- FAIL'd jobs trigger a retry workflow with exponential backoff.
- Contains a comprehensive Web UI for management and monitoring.

## How it works

Faktory listens on two ports:

* 7419 is the command port, clients AHOY and then PUSH or FETCH jobs.
* 7420 is the UI port for web browsers

See the [Security](/contribsys/faktory/wiki/Security) wiki page for
details about securing Faktory network access.

## Documentation

Please [see the Faktory wiki](https://github.com/contribsys/faktory/wiki) for full documentation.

## License

Faktory is licensed GPLv3.

## Author

Mike Perham, @mperham
