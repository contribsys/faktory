# Faktory Changelog

Changelog: Faktory || [Faktory Enterprise](https://github.com/contribsys/faktory/blob/master/Ent-Changes.md)

## 1.9.0

- Implement native TLS support [#469]
  Put `public.cert.pem` and `private.key.pem` in your config directory
  and Faktory will automatically enable TLS on port 7419 and 7420.
- Unwrap and display ActiveJob class names [#460, ibrahima]
- Add Go client API so batches can push jobs in bulk [#437, tylerian]

## 1.8.0

- Upgrade to Go 1.21 and go-redis 9.2.0.
- Don't double encode HTML entities for display in Web UI [#440]
- Validate and limit `days` parameter for Dashboard [#431] CVE-2023-37279
- Validate and limit `timeInterval` refresh parameter for Dashboard

## 1.7.0

- Upgrade all internal APIs to propogate Context
- Upgrade `redis/go-redis` to the new v9 driver, requires Redis v6.0+ and RESP3
- Upgrade `datadog-go` to the new v5 driver
- Upgrade to Go 1.20 and Alpine 3.17

## 1.6.2

- Add `client.RemoveQueues(...)` to delete existing queues via API
- Add IO deadlines to client I/O [#373]
- Upgrade to Go 1.19

## 1.6.1

- Add per-worker connection count on Busy page
- Add `QUEUE REMOVE [queue_names...]` subcommand to remove one or more queues [#399]
- Fixes for linting warnings from gocritic and golangci-lint
- Upgrade to Go 1.18.

## 1.6.0

- The core Job struct in Go was modified slightly so the Retry element
  is now a `int*` rather than `int`. This was necessary so code could
  distinguish between the default value "0" and disabling retries "0";
  the default value is now "nil" and will result in the default retry
  policy of "25". [#385]
- Add `remaining` to the Job Failures struct to track retries remaining [#374]
- Add new `PUSHB` PUSH Bulk jobs command. You send an Array of Jobs
  rather than a single Job with `PUSH`. There is no limit to the Array
  size but we recommend 1000 at a time as a best practice. Returns a Map
  of JID to ErrorMsg for jobs which failed to push. [#386]
```
PUSHB [{job},{job},...] => Map<JID, ErrorMsg>
```

## 1.5.5

- Fix worker heartbeat monitoring which led to premature worker disassociation

## 1.5.4

- Remove invalid client-side deadline which lead to lots of I/O errors [#375]
- Upgrade to Go 1.17.

## 1.5.3

- Remove trailing "-1" on Faktory versions and tags. Future versions
  will use the standard form of "vX.Y.Z" except for Linux DEB/RPM packages.
- Implement client-side I/O deadlines, which will look like this if the
  network is slow or misconfigured:
  `read tcp 127.0.0.1:63027->127.0.0.1:7419: i/o timeout`
- Build fixes for ARM support [#370]

## 1.5.2

- Extend Faktory's Redis load timeout to allow for huge datasets [#225]
- Upgrade Bootstrap from v3.3.7 to v5.0.1 [#360, saurori]
- Support username in FAKTORY_URL, useful for connection routing proxies.
- Publish arm64 Docker images [#366]

## 1.5.1

- **Change license from GPLv3 to AGPLv3.** This is intended to ensure Faktory
  remains open source, even if someone forks Faktory and provides it as a
  service. **No worker or application code is covered by this license.**
- Add support for native arm64 builds. Docker images still in progress.
- Fixed Windows build.

## 1.5.0

- Add `QUEUE PAUSE` and `QUEUE RESUME` commands, add buttons to the /queues page [#336]
- The Busy page can now display current RSS for Worker processes [#339]
- The /debug page now displays Redis latency [#337]
- Add `Dialer` support for custom `faktory.Client` connections [#330]
- Upgrade to Go 1.16 and alpine 3.13; remove go-bindata dependency [#338]

## 1.4.2

- Allow FAKTORY_ENV to switch environment without flags [#325]
- Fix jobs with Retry:0 not running FAIL middleware [#317]
- Fix frequent "Bad connection EOF" log messages due to k8s probes
- Upgrade to Go 1.15 and alpine 3.12.

## 1.4.1

- Fix crash with invalid mutate API usage [#313]
- Fix form handler on Scheduled Job and Retry Job UI pages [#236]
- Refactor scheduled job processing to handle large job counts [#309]

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
