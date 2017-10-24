# Faktory

At a high level, Faktory is a work server.  It is the repository for
background jobs within your application. Jobs have a type and a set of
arguments and are placed into queues for workers to fetch and execute.

You can use this server to distribute jobs to one or hundreds of
machines. Jobs can be executed with any language by clients using
the Faktory API to fetch a job from a queue.

![webui](https://raw.githubusercontent.com/contribsys/faktory/master/misc/webui.png)

## Basic Features

- Jobs are represented as JSON hashes.
- Jobs are pushed to and fetched from queues.
- Jobs are reserved with a timeout, 30 min by default.
- Jobs not ACK'd or FAIL'd within the reservation timeout are requeued.
- FAIL'd jobs trigger a retry workflow with exponential backoff.
- Contains a comprehensive Web UI for management and monitoring.

## Installation

See the [Installation wiki page](https://github.com/contribsys/faktory/wiki/Installation) for current installation methods.

**We need help getting Faktory running easily on OSX.**  Today you have to
git clone this repo and [build manually](https://github.com/contribsys/faktory/wiki/Development) but we hope to get it into
Homebrew soon.

## Documentation

Please [see the Faktory wiki](https://github.com/contribsys/faktory/wiki) for full documentation.

## Support

You can also find help in the [contribsys/faktory](https://gitter.im/contribsys/faktory) chat channel. Stop by and say hi!

## Author

Mike Perham, @mperham, mike @ contribsys.com
