# Faktory [![CI](https://github.com/contribsys/faktory/actions/workflows/ci.yml/badge.svg)](https://github.com/contribsys/faktory/actions/workflows/ci.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/contribsys/faktory)](https://goreportcard.com/report/github.com/contribsys/faktory)

At a high level, Faktory is a work server.  It is the repository for
background jobs within your application. Jobs have a type and a set of
arguments and are placed into queues for workers to fetch and execute.

You can use this server to distribute jobs to one or hundreds of
machines. Jobs can be executed with any language by clients using
the Faktory API to fetch a job from a queue.

![webui](https://raw.githubusercontent.com/contribsys/faktory/master/docs/webui.png)

## Basic Features

- Jobs are represented as JSON hashes.
- Jobs are pushed to and fetched from queues.
- Jobs are reserved with a timeout, 30 min by default.
- Jobs `FAIL`'d or not `ACK`'d within the reservation timeout are requeued.
- FAIL'd jobs trigger a retry workflow with exponential backoff.
- Contains a comprehensive Web UI for management and monitoring.

## Installation

See the [Installation wiki page](https://github.com/contribsys/faktory/wiki/Installation) for current installation methods.
Here's more info on installation with [Docker](https://github.com/contribsys/faktory/wiki/Docker) and [AWS ECS](https://github.com/contribsys/faktory/wiki/AWS-ECS).

## Documentation

Please [see the Faktory wiki](https://github.com/contribsys/faktory/wiki) for full documentation.

## Support

Open up a Discussion or Issue.

## Author

Mike Perham, @getajobmike, mike @ contribsys.com
