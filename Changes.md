# Faktory Changelog

Changelog: Faktory || [Faktory Pro](https://github.com/contribsys/faktory/blob/master/Pro-Changes.md) || [Faktory Enterprise](https://github.com/contribsys/faktory/blob/master/Ent-Changes.md)

## 1.4.0

- Faktory Enterprise now supports **[Job Tracking](https://github.com/contribsys/faktory/wiki/Ent-Tracking)**.
- Integrate golangci-lint and numerous minor changes for issues it raised
- Support staging environment `-e staging`

## 1.3.0

- Allow workers to send their current process state as part of BEAT [#266]
- Faktory will now dump all thread stacktraces when sent the TTIN signal
- Remove apex/log dependency [#289]
- Upgrade to Go 1.14
- Enable SLOWLOG in Faktory's Redis instance

## 1.2.0

- **Faktory Enterprise** is now available with [Batches and Queue Throttling](https://github.com/contribsys/faktory/wiki#faktory-enterprise).
- Upgrade Docker image to Alpine 3.10
- Add connection pool support for `client` Go package.
- Refactor manager package for Pro/Ent integration.
- Scheduled jobs now go through `push` middleware when pushed [#260]

## 1.1.0

- Upgrade Go runtime from 1.10 to 1.13.
- Faktory, Faktory Pro and faktory\_worker\_go are now using Go modules [#216, ClaytonNorthey92]
- Fix "Add to Queue" button on the Scheduled page [#236]
- Rework fetching jobs from queues to be more efficient. [#235]
- Add several helper APIs to configure Faktory Pro features. [#251, tylerb]
- You can now put nginx in front of Faktory's Web UI using `proxy_pass`:
```nginx
location /faktory {
   proxy_set_header X-Script-Name /faktory;

   proxy_pass   http://127.0.0.1:7420;
   proxy_set_header Host $host;
   proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
   proxy_set_header X-Scheme $scheme;
   proxy_set_header X-Real-IP $remote_addr;
}
```

## 1.0.1

- Pause Faktory boot and wait upon any `LOADING` errors from Redis

## 1.0.0

- Support known, specific errors to be returned to clients [#208]
- All jobs now default to `"retry":25` if not specified when pushed [#211]
- Update job arguments within Web UI to show as JSON, not native Go [#210]

## 0.9.7

- Add new MUTATE command which allows bulk manipulation of Faktory's
  persistent sets (i.e. scheduled, retries, dead) [#205]
  See https://github.com/contribsys/faktory/wiki/Mutate-API

## 0.9.6

- Remove legacy job priority from APIs and Job struct
- Improve display of job arguments and custom elements in Web UI [#199]

## 0.9.5

- Add queue sizes to the INFO stats [#197, thapakazi]
- Fix infinite loop with sorted set iteration [#196, antoinefinkelstein]

## 0.9.4

- More aggressive Redis persistence [#195]
- Fix possible race condition panic on new connection

## 0.9.3

- Increase maximum client count to 1000.

## 0.9.2

- Fix crash under load [#187]
- First [Faktory Pro](https://contribsys.com/faktory) release

## 0.9.1

- Fix crash on startup in Linux in development mode
- Close all associated connections when a worker process expires [#182]
- Shutdown Redis cleanly if Faktory panics (Linux only)

## 0.9.0

- Switch Faktory from RocksDB to Redis for storage. [#160]
- Implement Faktory-internal middleware hooks [#168]
- Integrate TOML config system [#169]

## 0.8.0

- Workaround for negative busy/retry/scheduled/dead counts [#148]
- Fix slow index page rendering under heavy load [#156]
- Upgrade to Go 1.10.3
- Upgrade to RocksDB 5.14.2

## 0.7.0

- Upgrade RocksDB from 5.7.3 to 5.9.2
- Add CSRF protection to Web UI [#92, vosmith]
- Faktory in production mode now requires a password by default [#113]
- Orphaned jobs now use the retry process so they don't re-enqueue forever [#99]
- Implement automated backups, default once per hour and keeping 24. [#106]
- Implement `purge` command for faktory-cli, to remove old backups.

## 0.6.1

- Fix job reservation [#94, jweslley]
- Send exhausted jobs to the morgue [#86, jweslley]

## 0.6.0

- Add support for job priorities [#68, andrewstucki]
  Jobs may now include "priority" with values 1-9, 9 being
  highest priority.  Push a job with `"priority":9` and it will
  effectively go to the front of the queue.  The default priority is 5.
```
{"jid":"12o31i2u3o1","jobtype":"FooJob","args":[1,2,3],"priority":8}
```
- Various protocol changes [#72]
- Remove TLS support in server [#76]
- Fix heartbeat pruning so old workers disappear from Busy page [#37]
- Add Docker image [#13]
- Add Homebrew install [#10]
- Lots of polish and code cleanup from cdrx, agnivade, adomokos,
  ustrajunior, jwsslley and others.  Thank you!

## 0.5.0

- Initial release
