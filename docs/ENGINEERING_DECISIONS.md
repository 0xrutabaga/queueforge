# Engineering decisions

Each entry gives the decision, the alternatives considered, and what the decision costs. File
and function names refer to this repository.

## 1. Claim with `FOR UPDATE SKIP LOCKED` in one statement

`_CLAIM` in `src/queueforge/queue.py` is one statement made of three CTEs. The first selects
runnable ids with `FOR UPDATE SKIP LOCKED`. The second updates them to `running` with a fresh
`gen_random_uuid()` lease token. The third inserts the matching `queueforge_attempts` rows.

- *Why one statement:* the lock, the state change and the history row commit together. No
  transaction stays open while a handler runs.
- *Why SKIP LOCKED:* concurrent claimers each take different rows instead of queueing behind one
  row lock. Under READ COMMITTED, a locked row that changed since the snapshot is re-checked
  against the `WHERE` clause, so a job that just became `running` is not taken again.
- *Alternative rejected:* holding a transaction (or an advisory lock) open for the whole job.
  It is simple, but one connection per running job does not scale. It also loses the crash
  signal if a connection pooler keeps the session alive.
- *Cost:* `LIMIT` with `SKIP LOCKED` can return fewer rows than are available. The worker
  simply claims again on its next loop (`Worker._claim_loop`).

## 2. Leases plus fencing tokens, checked by the database clock

A claim sets `lease_expires_at = now() + lease`. Every later change for that claim goes through
`_LOCK_LEASE`, or through `_COMPLETE` / `_HEARTBEAT`. Each of these requires
`lease_token = $token AND state = 'running' AND lease_expires_at > clock_timestamp()`. If no
row matches, the caller gets `LeaseLost`. The reaper uses the mirror condition,
`lease_expires_at <= clock_timestamp()`.

- *Why the expiry check as well as the token:* without it, a worker whose lease expired could
  still complete the job until the reaper ran. With it, "valid lease" means the same thing
  everywhere, and the result does not depend on when the reaper last ran
  (`test_completion_after_lease_expiry_is_rejected_even_before_reaping`).
- *Why the database clock:* workers on different hosts disagree about time. One server's
  clock does not.
- *Why `clock_timestamp()` and not `now()`:* `now()` is the start time of the transaction.
  `complete(conn=...)` runs inside the caller's transaction, which may have started well before
  the lease ran out. With `now()`, a completion issued after expiry was accepted. A review found
  this; the regression test is
  `test_completion_in_a_long_caller_transaction_checks_the_current_time`. New lease deadlines
  are still computed from `now()`, but those statements run in short pool transactions.
- *Commit after the check:* once `complete(conn=...)` has updated the row, the caller's
  transaction holds its lock until COMMIT. The reaper uses `SKIP LOCKED`, so it cannot take the
  row in that window (`test_completion_in_a_caller_transaction_holds_the_row_until_commit`).
- *Cost:* a worker that pauses past its lease loses work it had already finished. That is the
  correct trade for a queue whose job is to avoid two owners.
- *Downstream fencing:* the UUID token only protects the queue's own rows. For external systems,
  `JobContext.fencing_token` returns `(job_id, attempt)`. `attempt` is monotonic: the trigger
  rejects any decrease, and `release` deliberately does not decrement it.

## 3. The state machine lives in SQL

`queueforge_transitions` lists the legal `(from, to)` pairs. The trigger
`queueforge_check_job_change()` (`migrations/0001_initial.sql`) rejects anything else. It also
rejects inserts in a state other than `queued`, decreasing `attempts`, and a new lease token on
an already-running job. CHECK constraints tie the lease columns to `state = 'running'` and
`finished_at` to the terminal states.

- *Why in the database:* operators run ad hoc SQL and future code has bugs. The Python methods
  already guard their `WHERE` clauses, so the trigger is a second, independent layer. It proved
  useful: with `SKIP LOCKED` removed from `_CLAIM`, the trigger turns a would-be double claim
  into an error instead of silent corruption.
- *Cost:* one trigger call and one index probe per row update. This was not measured
  separately; `scripts/bench.py` includes it.
- The README diagram is checked against the table by
  `tests/test_schema.py::test_readme_state_diagram_matches_sql_transitions`.

## 4. `failed` is a waiting state, not a terminal one

`failed` means a retry is scheduled at `run_at`. Claims take `state IN ('queued', 'failed') AND
run_at <= now()`. This keeps "this job has had trouble" visible in `stats()` and lets an
operator cancel a job that is waiting for a retry. `dead` is the terminal failure state
(dead-letter).

## 5. Backoff computed in Python, stored per job

`JobQueue._after_failure` calls `full_jitter_delay` (`src/queueforge/backoff.py`):
`uniform(0, min(cap, base * 2^(attempt-1)))`. The policy (`max_attempts`, base, cap) is stored
on each job row at enqueue time.

- *Why full jitter:* when many jobs fail at the same moment (a dependency outage), full jitter
  spreads their retries over the whole window instead of in synchronised waves.
- *Why per job:* the reaper on any worker settles the job with the policy the producer chose.
  This does not depend on how that worker is configured.
- *Why Python:* the random source can be injected (`JobQueue(rng=...)`), which makes the
  schedule test exact (`test_failure_schedules_a_retry_with_backoff`). It costs one extra
  `SELECT ... FOR UPDATE` per failure, which happens only on the error path.

## 6. Crash recovery by a reaper that every worker runs

`JobQueue.reap_expired` selects expired running jobs of its queue with `FOR UPDATE SKIP LOCKED`.
It then settles each one with the same rule as a failure (`_after_failure`): retry, dead, or
cancelled if cancellation was requested. The attempt outcome is recorded as `lease_expired`. A
crash therefore counts as an attempt, so a job that kills its worker every time ends up dead
instead of looping forever.

- *Alternative:* let claims take expired running jobs directly. That mixes two concerns and
  makes the attempt history less clear.

## 7. Cooperative cancellation and shutdown; no release of running threads

Python threads cannot be killed. So:

- `cancel()` on a running job only sets `cancel_requested`. The heartbeat thread
  (`Worker._heartbeat_loop`) reads it and calls `ctx.request_stop(StopReason.CANCELLED)`. The
  handler raises `JobInterrupted` from `ctx.check()` / `ctx.sleep()`, and
  `Worker._settle_interrupted` acknowledges the cancel.
- On shutdown, `Worker._drain` asks every handler to stop and waits up to the grace period.
  Handlers that stopped are released back to `queued`. Handlers that did not stop are **not**
  released. Releasing them would let another worker start the job while this thread still runs.
  Their leases are left to expire
  (`test_shutdown_leaves_uncooperative_jobs_to_lease_expiry`).
- A handler that returns normally after a cancel request is recorded as `succeeded`: the work
  was done.

## 8. Stop the handler when the lease cannot be renewed

If heartbeats fail (database unreachable, network partition), the database lease expires and
another worker may claim the job. `JobContext.lease_deadline` is a local monotonic deadline,
measured from just *before* each claim or heartbeat request, so it never falls later than the
real `lease_expires_at`. Once it passes, the heartbeat loop stops the handler with
`LEASE_LOST` (`test_handler_is_stopped_when_heartbeats_keep_failing`). This narrows the zombie
window for cooperative handlers. It cannot close it for handlers that never check `ctx`.

## 9. Idempotency keys with conflict detection

There is a partial unique index on `(queue, idempotency_key)`. `_ENQUEUE` uses
`ON CONFLICT ... DO NOTHING RETURNING id`, followed by `_FIND_BY_KEY`. When a concurrent insert
with the same key is still in flight, PostgreSQL waits for it before deciding, so exactly one
producer gets `created=True` (`test_concurrent_enqueues_with_one_key_create_one_job`). Reusing a
key with a different task or payload raises `IdempotencyConflict` rather than silently returning
a job that does something else.

## 10. Transactional enqueue and completion

`enqueue(conn=...)` and `complete(conn=...)` run on the caller's connection and do not commit.
A job can then be created atomically with the business row that caused it (outbox pattern).
A handler's writes to this same database can be committed together with its completion: the
writes become effectively-once, while the queue as a whole is still at-least-once.

A caller transaction at REPEATABLE READ or SERIALIZABLE cannot see an idempotency key that
another transaction committed after its snapshot. PostgreSQL then fails the
`INSERT ... ON CONFLICT DO NOTHING` with a serialization failure (SQLSTATE 40001) rather than
returning nothing. queueforge lets that error propagate, and the caller retries the
transaction (`test_repeatable_read_caller_sees_a_serialization_failure_on_a_concurrent_key`).

## 11. Polling instead of LISTEN/NOTIFY

Workers poll every `poll_seconds` when idle. When a claim fills all free slots, they claim again
immediately. Polling is simple and has no missed-notification edge cases, and the partial index
`queueforge_jobs_runnable` keeps an empty poll cheap. NOTIFY is listed as future work.

## 12. Threads, a connection pool, one transaction per call

`JobQueue` borrows a connection from a `psycopg_pool.ConnectionPool` for each call, so it is
thread-safe.

The worker tracks running handlers by **lease token**, not job id. After a lost lease, the same
worker may claim attempt 2 of a job while attempt 1's thread is still running. When the
tracking was keyed by job id, attempt 1 finishing removed attempt 2's entry. Attempt 2 then got
no heartbeats, its lease expired, and attempt 3 started while attempt 2 was still running. A
review found this; the regression test is
`tests/test_worker.py::test_stale_attempt_finishing_does_not_drop_the_new_attempts_heartbeats`.
Stale threads still count against `concurrency` and are waited for on shutdown, because they
really are running. The worker needs about `concurrency + 2` connections. Handler threads are daemon
threads, so a process can exit even if a handler is stuck. The CLI then exits with status 1 to
signal abandoned jobs.
