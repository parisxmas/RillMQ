# A job queue in Python

A producer puts jobs on a queue and two workers share them out, using `pika`,
the Python client written for RabbitMQ, without a line changed for RillMQ.

```sh
pip install pika
sh examples/jobs/run.sh        # from the repository's root, with ./rillmq built
```

What it shows:

- **Each job goes to one worker.** `basic_qos(prefetch_count=1)` means a worker
  is handed the next job only once it has acknowledged the last, so a slow one
  is not left holding a pile.
- **A job is safe once the producer returns.** `confirm_delivery()` makes each
  publish wait until the broker has the job on the disk, and the queue and the
  jobs are durable, so a broker that is restarted still has them.
- **A worker that dies does not lose its job.** One of the jobs is `crash`, and
  the worker that takes it exits without acknowledging it. The broker gives it
  to the other worker at once, marked `redelivered`, which is how a worker can
  tell a job that may be half done from a new one:

```
one: job 5
two: crash
two: dies without acknowledging it
one: crash (delivered before)
one: job 6
```
