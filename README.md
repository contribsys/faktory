# Faktory

At a high level, Faktory is a work server.  It is the repository for
background jobs within your application. Jobs have a type and a set of
arguments and are placed into queues for workers to fetch and execute.

You can use this server to distribute jobs to one or hundreds of
machines.  Jobs can be executed with any language by clients using
the Faktory API to fetch a job from a queue.

![webui](docs/webui.png)

## Basic Features

- Jobs are represented as JSON hashes.
- Jobs are pushed to and fetched from queues.
- Jobs are reserved with a timeout, 30 min by default.
- Jobs not ACK'd or FAIL'd within the reservation timeout are requeued.
- FAIL'd jobs trigger a retry workflow with exponential backoff.
- Contains a comprehensive Web UI for management and monitoring.

## Installation

Standard 64-bit Linux builds are distributed via PackageCloud.io.  For Debian and Ubuntu:

```
curl -s https://packagecloud.io/install/repositories/contribsys/faktory/script.deb.sh | sudo bash
sudo apt-get install faktory
```

See the [PackageCloud repository](https://packagecloud.io/contribsys/faktory) for RPM and other options.

**We need help getting Faktory running easily on OSX.**  Today you have to
git clone this repo and [build manually](wiki/Development) but we hope to get it into
Homebrew soon.

## Documentation

Please [see the Faktory wiki](/contribsys/faktory/wiki) for full documentation.

## Author

Mike Perham, @mperham, mike @ contribsys.com
