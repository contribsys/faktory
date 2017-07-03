## Worq Storage

By default, Worq stores everything in /var/run/worq.  The database
internally has several "timed sets" to store job data:

 * Scheduled - jobs which will be run at some point in the future,
   sorted by time.
 * Retries - failed jobs which will be run at some point in the future,
   sorted by time.
 * Working - jobs which are currently in progress by an executor, sorted
   by when they expire and will be returned to a public queue for
   re-execution.
 * Queues - jobs which are pending execution.

All operations are designed to be atomic and transactional, e.g. moving a
job from a queue to the working set is a transactional operation.
