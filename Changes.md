## HEAD

- Faktory in production mode now requires a password by default [#113]
- Expired jobs now use the retry process so they don't re-enqueue forever [#99]
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
