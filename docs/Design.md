# Thoughts

- decompose a business into smaller and smaller transactions.
- scale apps by turning their work into lots of small jobs
- process thousands of jobs across many machines
- jobs can be grouped, with success callback
- with groups, we can compose larger workflows

There is no "work" daemon / system / server today in the OSS world.  I aim
to build something like Redis: instead of a data structure server, I want
to build a job server.

## Alternatives

- Message queues are for routing individual messages.  They don't have
higher level constructs like groups which allow larger workflows.
- Background job frameworks are often "smart" clients that build on top of
dumb data servers: Sidekiq and Celery are examples.  With Sidekiq, I've built
everything I can to implement a useful work system but because it is a smart
client, everything is tied to Ruby and limited to whatever the datastore
supports.  Sidekiq's footprint and impact is bounded to the Ruby ecosystem.

## Build

Building it ourselves gives several advantages and disadvantages:

### Good

- language-independent, large impact, large ecosystem
- writing in Go means very easy deployment, zero dependencies
- dumber clients, much functionality can be implemented in server

### Bad

- building our own datastore, persistence, replication, high availability
  * can we leverage existing projects, e.g. BoltDB, to solve much of this?
- requires clients in each language
  * I expect clients to appear quickly if there is decent project uptake.
  * I can provide a Ruby client, reusing a lot of the Sidekiq codebase

