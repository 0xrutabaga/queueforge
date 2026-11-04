# Interview guide

Five questions a senior backend interviewer is likely to ask about this project. Each answer
points to the code that backs it, followed by tradeoffs and a live demo script.

## 1. "Can a job ever run on two workers at the same time?"

Two things need separating:

- **Two valid leases at once: no.** Only `JobQueue.claim` (`_CLAIM` in
  `src/queueforge/queue.py`) creates a lease. It only takes `queued`/`failed` rows, under
  `FOR UPDATE SKIP LOCKED`. As a second layer, the trigger `queueforge_check_job_change()`
  rejects a new token on a running job. Tests: `tests/test_concurrency.py`. One test has 12
  threads racing raw claims. Another runs 4 in-process workers with 20% injected failures. A
  third runs 3 worker *processes*, where each handler takes `pg_try_advisory_lock(job_id)` and
  records a violation if another session holds it.
- **Two handler executions at once: possible after a lease is lost.** If a worker pauses longer
  than its lease, another worker can claim the job while the first thread still runs. The queue
  fences the stale attempt: `_COMPLETE` and `_LOCK_LEASE` require the current token and a
  lease that has not expired by `clock_timestamp()`, so its result is rejected with
  `LeaseLost`. But the queue cannot undo external side effects. Mitigations in code:
  `JobContext.lease_deadline` stops cooperative handlers once renewals have failed for a lease
  period, `Worker._drain` never releases a still-running job, and `JobContext.fencing_token` is
  available for downstream checks. The honest summary is at-least-once with fencing, not
  exactly-once.
- **Two bugs an independent review found here, both now fixed with regression tests.**
  - The worker tracked running handlers by job id. When a stale attempt finished, it removed
    the entry for the live next attempt on the same worker, so that attempt lost its heartbeats
    and a third attempt overlapped it. The worker now tracks them by lease token
    (`test_stale_attempt_finishing_does_not_drop_the_new_attempts_heartbeats`).
  - The expiry checks used `now()`, the transaction start time. That let
    `complete(conn=...)` succeed after the lease had expired inside a long caller transaction.
    They now use `clock_timestamp()`.

## 2. "Walk me through a worker crash."

1. The worker is killed after `claim`. The row stays `running` with `lease_expires_at` in the
   future, and its attempt row has no outcome.
2. Nobody renews the lease, because heartbeats came from the dead process
   (`Worker._heartbeat_loop`).
3. Another worker's claim loop calls `JobQueue.reap_expired` every `lease/2`. That method takes
   expired rows with `SKIP LOCKED` and settles them through `_after_failure`: `failed` with a
   backoff `run_at`, `dead` if attempts are exhausted, or `cancelled` if a cancel was pending.
   The attempt is marked `lease_expired`.
4. A later claim re-runs it as attempt 2 with a new token.

Proof: `tests/test_crash.py::test_killed_worker_job_is_rerun_after_lease_expiry` kills a real
subprocess with `Popen.kill()` (SIGKILL / TerminateProcess) and asserts the history
`[("doomed", "lease_expired"), ("rescuer", "succeeded")]`. The demo does the same thing on a
larger workload.

Tradeoff: recovery time is between the lease length and the lease plus the reap interval. A
shorter lease recovers faster but needs more heartbeat UPDATEs and is less tolerant of pauses.

## 3. "Why not just use a long transaction or an advisory lock per job?"

A transaction held for the whole job ties up one connection per running job. It also keeps an
old snapshot open, which holds back vacuum, and it has no visible state for operators. Advisory
locks have the same connection cost and do not survive poolers in transaction mode. Leases make
ownership an explicit, queryable row (`stats()` reports `expired_leases`). They make crash
recovery an ordinary state transition, and keep every transaction short. The cost is the
heartbeat writes and the zombie window described in question 1. Details:
`docs/ENGINEERING_DECISIONS.md` sections 1 and 2.

## 4. "How do retries behave when a dependency goes down for everyone?"

`full_jitter_delay` in `src/queueforge/backoff.py` draws uniformly from
`[0, min(cap, base * 2^(attempt-1)))`. Synchronised failures therefore spread out instead of
retrying in waves. The policy is stored per job at enqueue time, so the reaper on any worker
applies the producer's policy. `PermanentError` skips retries entirely. A job that always fails
ends `dead` after `max_attempts`, with `last_error` and the full attempt history kept.
`requeue_dead` lets an operator redrive it after a fix; it raises `max_attempts` rather than
resetting `attempts`, so the fencing number stays monotonic.

The worker itself survives the outage. `Worker._claim_loop` catches `psycopg.Error` and backs
off (`test_worker_survives_a_database_outage`). `JobContext.lease_deadline` stops handlers whose
leases can no longer be renewed (`test_handler_is_stopped_when_heartbeats_keep_failing`).

Tests: `test_failure_schedules_a_retry_with_backoff` checks the exact schedule 1, 2, 4, 5 s with
the random draw fixed at 1.0. `test_jittered_delays_stay_inside_the_backoff_window` checks real
jitter stays in bounds.

## 5. "How do you stop a producer from enqueueing the same thing twice?"

There is a partial unique index on `(queue, idempotency_key)`. `JobQueue.enqueue` runs
`INSERT ... ON CONFLICT DO NOTHING RETURNING id` and then looks up the existing row. If a
concurrent insert with the same key has not committed yet, PostgreSQL waits for it, so 16
racing producers produce exactly one `created=True`
(`test_concurrent_enqueues_with_one_key_create_one_job`). A different payload under the same key
raises `IdempotencyConflict` instead of being silently dropped. Producers can also pass
`conn=` to enqueue inside their own transaction, so a rolled-back order never leaves a job
behind (`test_enqueue_inside_a_rolled_back_transaction_leaves_no_job`).

Limit to state: keys deduplicate only while the job row exists. There is no retention job yet,
so today rows are kept forever. When retention is added, the dedup window will equal the
retention period.

## Tradeoffs to raise before being asked

- Polling, not NOTIFY: up to `poll_seconds` of pickup latency.
- Threads, not processes, per worker. Fine for I/O-bound handlers; run more processes for CPU
  work.
- Every heartbeat is an UPDATE. Table bloat and autovacuum need attention at high rates. The
  only throughput number in the repo comes from `scripts/bench.py` on one Windows desktop with
  Docker Desktop, and the README labels it as such.
- Strict priority can starve low-priority work.

## Live demo steps (about 5 minutes)

```bash
docker compose up -d --wait
uv run queueforge demo                    # kill, retries, dead-letter, cancellation, 10 checks
uv run queueforge --queue demo stats      # counts after the run
uv run queueforge --queue demo show <id of the demo.crash_victim job>   # lease_expired then succeeded
```

Then show fencing by hand in a Python shell:

```python
from queueforge import JobQueue, RetryPolicy, LeaseLost
from queueforge.cli import DEFAULT_DSN

q = JobQueue.connect(DEFAULT_DSN, "live")
job_id = q.enqueue("t", retry=RetryPolicy(3, 0, 0)).job_id
a = q.claim("worker-a", lease_seconds=2)[0]
import time

time.sleep(2.5)
q.reap_expired()  # [(job_id, <JobState.FAILED: 'failed'>)]
b = q.claim("worker-b")[0]
try:
    q.complete(job_id, a.lease_token)  # stale worker
except LeaseLost as e:
    print("rejected:", e)
q.complete(job_id, b.lease_token)  # current owner succeeds
[(x.worker_id, x.outcome) for x in q.attempts(job_id)]
```

Close by opening `src/queueforge/migrations/0001_initial.sql` to show the transition table and
trigger. Then run `uv run pytest -q tests/test_schema.py` to show that the README diagram is
checked against it.
