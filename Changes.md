# Faktory Changelog

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
