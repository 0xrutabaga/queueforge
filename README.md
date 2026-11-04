# queueforge

Background jobs have to survive worker crashes, retries and operator mistakes. The hard part is
making sure a job that a dead worker had started is run again, while a worker that is only slow
cannot overwrite the result of the attempt that replaced it. queueforge is a job queue library
and worker CLI on PostgreSQL. It handles this with `SELECT ... FOR UPDATE SKIP LOCKED` claims,
leases with heartbeats, and lease tokens that act as fencing tokens. The job state machine is
enforced by the database itself.

## Why it matters

Many teams already run PostgreSQL and do not want a second stateful system (a broker, Redis) just
for background work. A queue in the same database can be enqueued inside the business
transaction, so a job exists only if the order that caused it committed. But a naive table queue
has well-known failure modes:

- two workers pick the same row;
- a crashed worker leaves the job stuck in `running` forever;
- a worker that paused (GC, network partition) for longer than its lease wakes up and marks the
  job done after another worker already re-ran it;
- a poison job retries forever, or every failed job retries at the same instant.

Each of these is handled here and has a test that reproduces it against a real PostgreSQL 17.

## What it does

Delivery semantics, stated precisely:

- **At-least-once execution.** A handler can run more than once for the same job: after a worker
  crash, a lost lease, a timeout, or when the outcome could not be written. It is **not
  exactly-once**. Handlers must be idempotent, or use the fencing token described below.
- **At most one valid lease per job at any time.** Only a claim creates a lease. A claim only
  takes `queued` or `failed` rows, with a row lock and `SKIP LOCKED`.
- **Fencing.** Every state change made for a claim (heartbeat, complete, fail, release,
  acknowledge cancel) must present that claim's `lease_token` and happen before
  `lease_expires_at`, measured by the database clock. Otherwise it changes nothing and raises
  `LeaseLost`. A stale worker cannot complete, fail or release a job that has since been
  re-claimed.
- **Monotonic attempt number.** `attempts` never decreases; the trigger enforces this.
  `JobContext.fencing_token` is `(job_id, attempt)`, which downstream systems can use to reject
  writes from older attempts.

Features:

- Leases with batched heartbeats, and a reaper that settles expired leases. The reaper retries,
  dead-letters or cancels such jobs, and it is safe to run on every worker at once.
- Retries with capped exponential backoff and full jitter. The policy is stored per job:
  `max_attempts`, `base_seconds`, `cap_seconds`.
- Dead-letter state with the last error and a full per-attempt history
  (`queueforge_attempts`). Operators can redrive a dead job with `requeue`.
- Idempotency keys, unique per queue. Re-enqueueing with the same key returns the existing job.
  Reusing the key with a different task or payload raises `IdempotencyConflict`.
- Transactional enqueue and completion. Pass `conn=` to `enqueue` or `complete` to make them part
  of your own transaction.
- Scheduled jobs (`run_at` / `delay_seconds`), priorities (higher first, FIFO within a priority).
- Cancellation: immediate for queued or retry-waiting jobs. For running jobs it is cooperative:
  the next heartbeat tells the handler, which stops at its next `ctx.check()` or `ctx.sleep()`.
- Worker with thread concurrency, graceful shutdown on SIGINT/SIGTERM (and CTRL_BREAK on
  Windows), an optional per-job timeout, and resilience to database outages.
- Health query: counts by state, age of the oldest runnable job, leases awaiting the reaper.

### Job states

The transitions below are the rows of `queueforge_transitions`. The trigger
`queueforge_check_job_change()` rejects any other transition, and
`tests/test_schema.py::test_readme_state_diagram_matches_sql_transitions` fails if this diagram
and the SQL disagree.

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    queued --> running: claim
    queued --> cancelled: cancel
    failed --> running: claim after backoff
    failed --> cancelled: cancel
    running --> succeeded: complete
    running --> failed: error or lease expired, attempts left
    running --> dead: attempts exhausted or PermanentError
    running --> cancelled: cancel acknowledged
    running --> queued: released on shutdown
    dead --> queued: requeue
    succeeded --> [*]
    cancelled --> [*]
```

`failed` means "an attempt failed and a retry is scheduled at `run_at`". It is not terminal.

## Architecture

```mermaid
flowchart LR
    P[Producer<br/>JobQueue.enqueue] -->|INSERT, optionally in caller's txn| DB[(PostgreSQL<br/>queueforge_jobs<br/>queueforge_attempts)]
    subgraph Worker process
        C[claim loop] -->|SELECT FOR UPDATE SKIP LOCKED| DB
        C --> T1[handler thread]
        C --> T2[handler thread]
        H[heartbeat thread] -->|batched lease renewal,<br/>reads cancel_requested| DB
        R[reaper, every lease/2] -->|settle expired leases| DB
        T1 -->|complete / fail with lease token| DB
    end
```

| Piece | File | Role |
| --- | --- | --- |
| Schema | `src/queueforge/migrations/0001_initial.sql` | tables, CHECK constraints, transition table and trigger, partial indexes |
| Migrations | `src/queueforge/migrate.py` | applies `*.sql` in order, serialised by an advisory lock |
| Queue API | `src/queueforge/queue.py` (`JobQueue`) | every state change is one short transaction |
| Backoff | `src/queueforge/backoff.py` | `backoff_ceiling`, `full_jitter_delay`, `RetryPolicy` |
| Worker | `src/queueforge/worker.py` (`Worker`, `JobContext`, `Registry`) | claim loop, handler threads, heartbeats, reaper, shutdown |
| CLI | `src/queueforge/cli.py` | `migrate`, `worker`, `enqueue`, `stats`, `show`, `cancel`, `requeue`, `reap`, `demo` |
| Demo | `src/queueforge/demo.py` | real worker subprocesses, injected failures, a hard kill |

The design reasoning is in [docs/ENGINEERING_DECISIONS.md](docs/ENGINEERING_DECISIONS.md).

## Technology choices

- **PostgreSQL 17**: row locks with `SKIP LOCKED`, `gen_random_uuid()`, partial indexes,
  PL/pgSQL triggers. All lease times use the database clock, so worker clock skew does not matter.
- **psycopg 3 + psycopg_pool**, plain SQL. There is no ORM: the queue is a handful of statements
  whose locking behaviour has to be visible in review.
- **Threads for handlers.** Most job handlers wait on I/O. Each worker process runs up to
  `--concurrency` handler threads. CPU-bound handlers should run in several worker processes.
- **uv, ruff (incl. bandit `S` rules), mypy strict, pytest** with `pytest-timeout`.

## Installation

Requires Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
git clone <this repository> queueforge
cd queueforge
uv sync
docker compose up -d --wait        # PostgreSQL 17 on 127.0.0.1:55432
uv run queueforge migrate
```

The default connection string is
`postgresql://queueforge:queueforge@127.0.0.1:55432/queueforge`, which matches
`docker-compose.yml` (local development credentials only). Override it with `--dsn` or
`QUEUEFORGE_DSN`.

## Quick start

```bash
uv run queueforge demo
```

The demo uses its own queue, `demo`, and clears it first. It then:

1. starts a worker process, waits until it holds a job, and kills it (`Popen.kill()`: SIGKILL on
   Linux, TerminateProcess on Windows);
2. enqueues 31 more jobs: 20 that succeed, 4 that fail twice and then succeed, 2 that always
   fail, 1 that raises `PermanentError`, a scheduled job, a far-future job that is cancelled
   while queued, a long job that is cancelled while running, and a duplicate enqueue with the
   same idempotency key;
3. starts two more worker processes, waits for every job to reach a terminal state, and stops
   the workers with SIGTERM (CTRL_BREAK on Windows);
4. prints counts per state, every job's attempt history, and ten checks. It exits non-zero if any
   check fails.

Abridged output from one run. Job ids and which worker ran what vary between runs:

```text
[  0.5s] worker-doomed claimed job 9254 (attempt 1); killing the process now
[  0.5s] job 9254 is still 'running' with a lease that nobody will renew
[  0.6s] enqueued 31 more jobs; idempotency key 'order-42' twice -> job 9285 (created=True), job 9285 (created=False)
[  0.6s] cancelled job 9283 (scheduled an hour ahead) while queued -> cancelled
[  0.6s] started 2 worker process(es) with concurrency 4 each
[  1.5s] requested cancellation of running job 9284 -> still 'running' until it stops
[  3.9s] every job reached a terminal state
[  4.1s] graceful shutdown exit codes: {'worker-a': 0, 'worker-b': 0}

Final counts by state:
  queued     0
  running    0
  succeeded  27
  failed     0
  dead       3
  cancelled  2

Attempt histories:
  job 9254 demo.crash_victim  succeeded  #1 worker-doomed lease_expired -> #2 worker-b succeeded
  job 9275 demo.flaky         succeeded  #1 worker-a failed -> #2 worker-b failed -> #3 worker-a succeeded
  job 9279 demo.poison        dead       #1 worker-a failed -> #2 worker-b failed -> #3 worker-a failed
  job 9281 demo.invalid       dead       #1 worker-a failed
  job 9283 demo.ok            cancelled  -
  job 9284 demo.long_running  cancelled  #1 worker-b cancelled
  ...
10/10 checks passed
```

## Examples

Define handlers and run a worker:

```python
# myapp/jobs.py
from queueforge import JobContext, PermanentError, Registry

registry = Registry()

@registry.task("send_receipt")
def send_receipt(ctx: JobContext, payload: dict) -> None:
    if "email" not in payload:
        raise PermanentError("no email address")       # dead-letter now, no retries
    for chunk in render_chunks(payload):
        ctx.check()                                     # stop promptly if cancelled / shutting down
        mailer.send(chunk, idempotency_key=f"receipt-{ctx.job_id}")  # safe to repeat
```

```bash
uv run queueforge --queue mail worker --app myapp.jobs:registry --concurrency 8 --lease-seconds 30
```

Enqueue inside your own transaction, with an idempotency key:

```python
import psycopg
from queueforge import JobQueue, RetryPolicy

queue = JobQueue.connect(DSN, "mail")
with psycopg.connect(DSN) as conn:          # commits on exit
    conn.execute("INSERT INTO orders (id, email) VALUES (%s, %s)", (order_id, email))
    queue.enqueue(
        "send_receipt",
        {"order_id": order_id, "email": email},
        idempotency_key=f"receipt-{order_id}",
        retry=RetryPolicy(max_attempts=8, base_seconds=2, cap_seconds=600),
        conn=conn,                          # the job exists only if the order commits
    )
```

Operate it from the shell:

```bash
uv run queueforge --queue mail enqueue send_receipt --payload '{"order_id": 7}' --key receipt-7
uv run queueforge --queue mail stats          # counts, oldest runnable age, expired leases
uv run queueforge --queue mail show 42        # job + attempt history as JSON
uv run queueforge --queue mail cancel 42
uv run queueforge --queue mail requeue 42 --extra-attempts 3   # redrive a dead job
```

## Testing

```bash
docker compose up -d --wait
uv run pytest -q
```

The tests run against real PostgreSQL. Each test uses its own queue name, so tests do not
interfere with each other or with the demo. CI runs the same commands on ubuntu-latest with a
PostgreSQL 17 service container (`.github/workflows/ci.yml`), plus ruff, mypy, the demo and
`pip-audit` on the exported lock file.

| Claim | Test |
| --- | --- |
| No job runs on two workers at once | `tests/test_concurrency.py`: 300 jobs claimed by 12 racing threads; 400 jobs on 4 in-process workers with 20% injected failures; 200 jobs on 3 worker **processes**, checked with a per-job advisory lock |
| Crash, then lease expiry, then re-run | `tests/test_crash.py::test_killed_worker_job_is_rerun_after_lease_expiry` (hard-kills a worker subprocess) |
| Fencing rejects stale completion | `tests/test_lifecycle.py::test_fencing_rejects_the_stale_worker_after_another_worker_reclaimed`, `test_completion_after_lease_expiry_is_rejected_even_before_reaping`, `tests/test_worker.py::test_shutdown_leaves_uncooperative_jobs_to_lease_expiry` |
| Backoff schedule | `tests/test_backoff.py`, `tests/test_lifecycle.py::test_failure_schedules_a_retry_with_backoff` (exact 1, 2, 4, 5 s), `test_jittered_delays_stay_inside_the_backoff_window` |
| Dead-letter | `test_dead_letter_after_max_attempts_keeps_the_last_error`, `test_permanent_failure_dead_letters_immediately`, `tests/test_worker.py::test_worker_dead_letters_a_poison_job` |
| Cancellation | `test_cancel_*` in `tests/test_lifecycle.py`, `tests/test_worker.py::test_running_job_is_cancelled_cooperatively` |
| Idempotent enqueue | `tests/test_enqueue.py` (incl. 16 concurrent producers with one key) |
| Scheduled jobs and priority | `tests/test_enqueue.py::test_scheduled_job_is_not_claimed_before_run_at`, `test_claims_follow_priority_then_fifo` |
| State machine in SQL | `tests/test_schema.py` (illegal transitions, terminal states, lease invariants) |
| Graceful shutdown by signal | `tests/test_crash.py::test_signal_triggers_graceful_shutdown_and_release` |
| Database down | `tests/test_worker.py::test_worker_survives_a_database_outage`, `test_handler_is_stopped_when_heartbeats_keep_failing`, `tests/test_cli.py::test_unreachable_database_exits_with_a_clear_message_and_no_password` |

To check that the concurrency tests can fail, remove `FOR UPDATE SKIP LOCKED` from `_CLAIM` in
`queue.py`. Racing claims then try to re-lease running jobs. The trigger rejects that, and
`test_concurrent_claims_never_hand_out_a_job_twice` fails. Remove the token and expiry
conditions from `_COMPLETE` and four fencing tests fail.

### Throughput

`scripts/bench.py` measures no-op jobs on your machine:

```bash
uv run python scripts/bench.py --jobs 2000 --workers 4 --concurrency 8
```

One run, on Windows 11, Intel Core i5-12400F (12 logical CPUs), Python 3.12, PostgreSQL 17 in
Docker Desktop on the same machine: enqueue 358 jobs/s (one transaction per job, one thread),
claim+run+complete 827 jobs/s. These numbers mostly measure round trips to the database through
Docker Desktop's network layer. They are not a capacity claim for any other setup.

## Limitations

- **At-least-once, not exactly-once.** Side effects outside this database can repeat. A handler
  that ignores `ctx` and keeps running after its lease is lost can overlap with the next attempt.
  The queue rejects its result, but cannot undo what it did.
- **Overlap needs an expired lease.** Workers stop renewing and tell the handler to stop once
  heartbeats have failed for a full lease period. A handler that does not check `ctx` is still a
  zombie until it returns.
- **Polling.** Idle workers poll every `--poll-seconds` (default 1 s). There is no
  `LISTEN/NOTIFY` wake-up, so pickup latency is up to one poll interval.
- **One thread per in-flight job.** This suits I/O-bound handlers. Python threads cannot be
  killed, so a hung handler is only fenced, never stopped.
- **No retention policy.** Finished jobs and attempt rows stay until you delete them. Idempotency
  keys are deduplicated only while the row exists.
- **Strict priorities can starve** low-priority jobs under sustained high-priority load.
- **Update-heavy table.** Every heartbeat is an UPDATE. At high job rates autovacuum settings
  and the heartbeat interval need tuning; this has not been measured beyond `scripts/bench.py`.
- Releasing on shutdown keeps the attempt counter, so frequent restarts consume attempts.

## Future work

- `LISTEN/NOTIFY` to wake idle workers on enqueue.
- Retention job (delete finished jobs older than N days, keeping idempotency keys for a window).
- Handler access to the completion transaction in `Worker`. `JobQueue.complete(conn=...)` exists
  but the worker does not expose it yet.
- Rate limits and concurrency limits per task.
- Prometheus-style metrics endpoint from `JobQueue.stats()`.
- Benchmarks on Linux against a dedicated PostgreSQL host.

## License

MIT, see [LICENSE](LICENSE).
