"""The job queue: every state change is one short PostgreSQL transaction.

Concurrency rules this module relies on:

* Claims use ``SELECT ... FOR UPDATE SKIP LOCKED`` so concurrent workers never block on, or
  both take, the same row.
* A claim issues a fresh random ``lease_token``. Every later change made on behalf of that claim
  (heartbeat, complete, fail, release, acknowledge cancel) must present the token *and* happen
  before ``lease_expires_at``. Otherwise it changes nothing and raises :class:`LeaseLost`.
  That is the fencing check.
* All times come from the database clock (``now()``), so worker clock skew does not matter.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow, TupleRow, dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from queueforge.backoff import RetryPolicy, full_jitter_delay
from queueforge.errors import (
    IdempotencyConflict,
    InvalidTransition,
    JobNotFound,
    LeaseLost,
    QueueForgeError,
)
from queueforge.models import (
    Attempt,
    EnqueueResult,
    HeartbeatResult,
    Job,
    JobState,
    QueueStats,
)

MAX_ERROR_LENGTH = 4000

_ENQUEUE = """
INSERT INTO queueforge_jobs (
    queue, task, payload, priority, run_at,
    max_attempts, backoff_base_seconds, backoff_cap_seconds, idempotency_key
)
VALUES (
    %(queue)s, %(task)s, %(payload)s, %(priority)s,
    COALESCE(%(run_at)s::timestamptz, now() + make_interval(secs => %(delay)s::float8)),
    %(max_attempts)s, %(base)s, %(cap)s, %(key)s
)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id
"""

_FIND_BY_KEY = """
SELECT id, (task = %(task)s AND payload = %(payload)s) AS same
FROM queueforge_jobs
WHERE queue = %(queue)s AND idempotency_key = %(key)s
"""

_CLAIM = """
WITH next AS (
    SELECT id
    FROM queueforge_jobs
    WHERE queue = %(queue)s AND state IN ('queued', 'failed') AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
), claimed AS (
    UPDATE queueforge_jobs j
    SET state = 'running',
        attempts = j.attempts + 1,
        lease_token = gen_random_uuid(),
        lease_expires_at = now() + make_interval(secs => %(lease)s::float8),
        locked_by = %(worker)s,
        heartbeat_at = now()
    FROM next
    WHERE j.id = next.id
    RETURNING j.*
), history AS (
    INSERT INTO queueforge_attempts (job_id, attempt, worker_id, lease_token)
    SELECT id, attempts, locked_by, lease_token FROM claimed
)
SELECT * FROM claimed ORDER BY priority DESC, run_at, id
"""

_HEARTBEAT = """
UPDATE queueforge_jobs j
SET lease_expires_at = now() + make_interval(secs => %(lease)s::float8),
    heartbeat_at = now()
FROM unnest(%(ids)s::bigint[], %(tokens)s::uuid[]) AS l(id, token)
WHERE j.id = l.id
  AND j.lease_token = l.token
  AND j.state = 'running'
  AND j.lease_expires_at > now()
RETURNING j.id, j.cancel_requested
"""

_COMPLETE = """
WITH done AS (
    UPDATE queueforge_jobs
    SET state = 'succeeded', lease_token = NULL, lease_expires_at = NULL, locked_by = NULL,
        finished_at = now()
    WHERE id = %(id)s
      AND lease_token = %(token)s
      AND state = 'running'
      AND lease_expires_at > now()
    RETURNING id, attempts
), history AS (
    UPDATE queueforge_attempts a
    SET finished_at = now(), outcome = 'succeeded'
    FROM done
    WHERE a.job_id = done.id AND a.attempt = done.attempts
)
SELECT id FROM done
"""

# Lock a job row on behalf of a lease holder; returns nothing if the lease is not valid.
_LOCK_LEASE = """
SELECT id, attempts, max_attempts, backoff_base_seconds, backoff_cap_seconds,
       cancel_requested, locked_by
FROM queueforge_jobs
WHERE id = %(id)s
  AND lease_token = %(token)s
  AND state = 'running'
  AND lease_expires_at > now()
FOR UPDATE
"""

_LOCK_EXPIRED = """
SELECT id, attempts, max_attempts, backoff_base_seconds, backoff_cap_seconds,
       cancel_requested, locked_by
FROM queueforge_jobs
WHERE queue = %(queue)s AND state = 'running' AND lease_expires_at <= now()
ORDER BY lease_expires_at
LIMIT %(limit)s
FOR UPDATE SKIP LOCKED
"""

_END_ATTEMPT_JOB = """
UPDATE queueforge_jobs
SET state = %(state)s,
    run_at = CASE
        WHEN %(delay)s::float8 IS NULL THEN run_at
        ELSE now() + make_interval(secs => %(delay)s::float8)
    END,
    lease_token = NULL,
    lease_expires_at = NULL,
    locked_by = NULL,
    last_error = COALESCE(%(error)s, last_error),
    finished_at = CASE WHEN %(terminal)s::boolean THEN now() END
WHERE id = %(id)s
"""

_END_ATTEMPT_HISTORY = """
UPDATE queueforge_attempts
SET finished_at = now(), outcome = %(outcome)s, error = %(error)s
WHERE job_id = %(id)s AND attempt = %(attempt)s
"""

_CANCEL = """
UPDATE queueforge_jobs
SET state = CASE WHEN state IN ('queued', 'failed') THEN 'cancelled' ELSE state END,
    finished_at = CASE WHEN state IN ('queued', 'failed') THEN now() ELSE finished_at END,
    cancel_requested = true
WHERE id = %(id)s AND state IN ('queued', 'failed', 'running')
RETURNING state
"""

_REQUEUE_DEAD = """
UPDATE queueforge_jobs
SET state = 'queued', run_at = now(), finished_at = NULL, cancel_requested = false,
    max_attempts = attempts + %(extra)s
WHERE id = %(id)s AND state = 'dead'
RETURNING id
"""

_STATS_COUNTS = """
SELECT state, count(*) AS n FROM queueforge_jobs WHERE queue = %(queue)s GROUP BY state
"""

_STATS_AGES = """
SELECT
    (SELECT extract(epoch FROM now() - min(run_at))::float8
     FROM queueforge_jobs
     WHERE queue = %(queue)s AND state IN ('queued', 'failed') AND run_at <= now())
        AS oldest_runnable_age_seconds,
    (SELECT count(*)
     FROM queueforge_jobs
     WHERE queue = %(queue)s AND state = 'running' AND lease_expires_at <= now())
        AS expired_leases
"""


@dataclass(frozen=True, slots=True)
class _LeaseRow:
    id: int
    attempts: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    cancel_requested: bool
    locked_by: str | None


def _truncate(error: str | None) -> str | None:
    if error is None or len(error) <= MAX_ERROR_LENGTH:
        return error
    return error[: MAX_ERROR_LENGTH - 15] + "... [truncated]"


class JobQueue:
    """Client for one named queue in a PostgreSQL database.

    Thread-safe: each call borrows a connection from the pool for one transaction.
    """

    def __init__(
        self,
        pool: ConnectionPool[psycopg.Connection[TupleRow]],
        queue: str = "default",
        *,
        rng: Callable[[], float] | None = None,
    ) -> None:
        if not queue:
            raise ValueError("queue name must not be empty")
        self._pool = pool
        self.queue = queue
        # Non-cryptographic randomness is correct here: it only spreads retry times.
        self._rng = rng or random.Random().random  # noqa: S311

    @classmethod
    def connect(
        cls,
        conninfo: str,
        queue: str = "default",
        *,
        max_connections: int = 4,
        pool_timeout: float = 10.0,
        rng: Callable[[], float] | None = None,
    ) -> JobQueue:
        """Create a queue client that owns its own connection pool. Call close() when done."""
        pool: ConnectionPool[psycopg.Connection[TupleRow]] = ConnectionPool(
            conninfo,
            min_size=1,
            max_size=max(1, max_connections),
            timeout=pool_timeout,
            open=True,
            name=f"queueforge-{queue}",
        )
        return cls(pool, queue, rng=rng)

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def _cursor(
        self, conn: psycopg.Connection[Any] | None = None
    ) -> Iterator[psycopg.Cursor[DictRow]]:
        if conn is not None:
            # Caller owns the transaction (for example to enqueue atomically with its own writes).
            with conn.cursor(row_factory=dict_row) as cur:
                yield cur
            return
        with self._pool.connection() as pooled, pooled.cursor(row_factory=dict_row) as cur:
            yield cur

    # ------------------------------------------------------------------ producers

    def enqueue(
        self,
        task: str,
        payload: Mapping[str, Any] | None = None,
        *,
        priority: int = 0,
        run_at: datetime | None = None,
        delay_seconds: float = 0.0,
        retry: RetryPolicy | None = None,
        idempotency_key: str | None = None,
        conn: psycopg.Connection[Any] | None = None,
    ) -> EnqueueResult:
        """Insert a job, or return the existing one if `idempotency_key` was already used.

        Reusing a key with a different task or payload raises IdempotencyConflict. Pass `conn`
        to enqueue inside the caller's transaction: the job then exists only if it commits.
        """
        if not task:
            raise ValueError("task must not be empty")
        if idempotency_key == "":
            raise ValueError("idempotency_key must not be empty")
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if run_at is not None and run_at.tzinfo is None:
            raise ValueError("run_at must be timezone-aware")
        policy = retry or RetryPolicy()
        params = {
            "queue": self.queue,
            "task": task,
            "payload": Jsonb(dict(payload or {})),
            "priority": priority,
            "run_at": run_at,
            "delay": float(delay_seconds),
            "max_attempts": policy.max_attempts,
            "base": policy.base_seconds,
            "cap": policy.cap_seconds,
            "key": idempotency_key,
        }
        with self._cursor(conn) as cur:
            row = cur.execute(_ENQUEUE, params).fetchone()
            if row is not None:
                return EnqueueResult(job_id=row["id"], created=True)
            existing = cur.execute(_FIND_BY_KEY, params).fetchone()
            if existing is None:  # pragma: no cover - conflict row vanished mid-transaction
                raise QueueForgeError("idempotency conflict but no existing job found")
            if not existing["same"]:
                raise IdempotencyConflict(
                    f"idempotency key {idempotency_key!r} already used by job {existing['id']} "
                    "with a different task or payload"
                )
            return EnqueueResult(job_id=existing["id"], created=False)

    # ------------------------------------------------------------------ workers

    def claim(self, worker_id: str, *, limit: int = 1, lease_seconds: float = 30.0) -> list[Job]:
        """Lease up to `limit` runnable jobs for `worker_id`, highest priority first."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        with self._cursor() as cur:
            rows = cur.execute(
                _CLAIM,
                {
                    "queue": self.queue,
                    "limit": limit,
                    "lease": float(lease_seconds),
                    "worker": worker_id,
                },
            ).fetchall()
        return [Job.from_row(row) for row in rows]

    def heartbeat(
        self, leases: Sequence[tuple[int, UUID]], *, lease_seconds: float
    ) -> list[HeartbeatResult]:
        """Extend still-valid leases. Leases missing from the result were lost."""
        if not leases:
            return []
        with self._cursor() as cur:
            rows = cur.execute(
                _HEARTBEAT,
                {
                    "lease": float(lease_seconds),
                    "ids": [job_id for job_id, _ in leases],
                    "tokens": [token for _, token in leases],
                },
            ).fetchall()
        return [HeartbeatResult(row["id"], row["cancel_requested"]) for row in rows]

    def complete(
        self, job_id: int, lease_token: UUID, *, conn: psycopg.Connection[Any] | None = None
    ) -> None:
        """Mark the job succeeded. Raises LeaseLost if the lease is no longer valid.

        Pass `conn` to complete inside the caller's transaction, so the job's own database
        writes and its completion commit or roll back together.
        """
        with self._cursor(conn) as cur:
            row = cur.execute(_COMPLETE, {"id": job_id, "token": lease_token}).fetchone()
        if row is None:
            raise LeaseLost(f"job {job_id}: completion rejected, lease no longer valid")

    def fail(
        self, job_id: int, lease_token: UUID, error: str, *, permanent: bool = False
    ) -> JobState:
        """Record a failed attempt. Returns the new state: failed (retry scheduled) or dead.

        If cancellation was requested while the attempt ran, the job becomes cancelled instead.
        """
        with self._cursor() as cur:
            lease = self._lock_lease(cur, job_id, lease_token)
            state, delay = self._after_failure(lease, permanent=permanent)
            self._end_attempt(cur, lease, state, "failed", error, delay)
        return state

    def release(self, job_id: int, lease_token: UUID) -> JobState:
        """Give a job back without counting it as a failure (used on graceful shutdown).

        The attempt counter is not decremented: it is the job's fencing number.
        """
        with self._cursor() as cur:
            lease = self._lock_lease(cur, job_id, lease_token)
            state = JobState.CANCELLED if lease.cancel_requested else JobState.QUEUED
            self._end_attempt(cur, lease, state, "released", None, 0.0)
        return state

    def acknowledge_cancel(self, job_id: int, lease_token: UUID) -> None:
        """Finish a running job whose cancellation was requested and honoured by the handler."""
        with self._cursor() as cur:
            lease = self._lock_lease(cur, job_id, lease_token)
            if not lease.cancel_requested:
                raise InvalidTransition(f"job {job_id}: no cancellation was requested")
            self._end_attempt(cur, lease, JobState.CANCELLED, "cancelled", None, None)

    def reap_expired(self, *, limit: int = 100) -> list[tuple[int, JobState]]:
        """Settle running jobs whose lease expired: retry, dead-letter, or cancel them.

        Safe to run from every worker at once: rows are taken with SKIP LOCKED.
        """
        settled: list[tuple[int, JobState]] = []
        with self._cursor() as cur:
            rows = cur.execute(_LOCK_EXPIRED, {"queue": self.queue, "limit": limit}).fetchall()
            for row in rows:
                lease = _LeaseRow(**row)
                state, delay = self._after_failure(lease, permanent=False)
                error = f"lease expired (worker {lease.locked_by})"
                self._end_attempt(cur, lease, state, "lease_expired", error, delay)
                settled.append((lease.id, state))
        return settled

    # ------------------------------------------------------------------ operators

    def cancel(self, job_id: int) -> JobState:
        """Cancel a job. Returns the state after the call.

        queued/failed -> cancelled immediately. running -> stays running with cancel_requested
        set; the worker sees it on its next heartbeat and asks the handler to stop. Jobs already
        in a terminal state are left unchanged.
        """
        with self._cursor() as cur:
            row = cur.execute(_CANCEL, {"id": job_id}).fetchone()
            if row is not None:
                return JobState(row["state"])
        return self.get_job(job_id).state

    def requeue_dead(self, job_id: int, *, extra_attempts: int = 1) -> None:
        """Redrive a dead job: back to queued with `extra_attempts` more attempts allowed."""
        if extra_attempts < 1:
            raise ValueError("extra_attempts must be >= 1")
        with self._cursor() as cur:
            row = cur.execute(_REQUEUE_DEAD, {"id": job_id, "extra": extra_attempts}).fetchone()
        if row is None:
            state = self.get_job(job_id).state
            raise InvalidTransition(f"job {job_id} is {state}, only dead jobs can be requeued")

    def get_job(self, job_id: int) -> Job:
        with self._cursor() as cur:
            row = cur.execute("SELECT * FROM queueforge_jobs WHERE id = %s", (job_id,)).fetchone()
        if row is None:
            raise JobNotFound(f"job {job_id} not found")
        return Job.from_row(row)

    def attempts(self, job_id: int) -> list[Attempt]:
        with self._cursor() as cur:
            rows = cur.execute(
                "SELECT job_id, attempt, worker_id, lease_token, started_at, finished_at,"
                " outcome, error FROM queueforge_attempts WHERE job_id = %s ORDER BY attempt",
                (job_id,),
            ).fetchall()
        return [Attempt(**row) for row in rows]

    def stats(self) -> QueueStats:
        """Counts by state, age of the oldest runnable job, and leases awaiting the reaper."""
        params = {"queue": self.queue}
        with self._cursor() as cur:
            counts = {state.value: 0 for state in JobState}
            for row in cur.execute(_STATS_COUNTS, params).fetchall():
                counts[row["state"]] = row["n"]
            ages = cur.execute(_STATS_AGES, params).fetchone()
        assert ages is not None  # noqa: S101 - a scalar subquery SELECT always returns one row
        return QueueStats(
            queue=self.queue,
            counts=counts,
            oldest_runnable_age_seconds=ages["oldest_runnable_age_seconds"],
            expired_leases=ages["expired_leases"],
        )

    # ------------------------------------------------------------------ internals

    def _lock_lease(self, cur: psycopg.Cursor[DictRow], job_id: int, token: UUID) -> _LeaseRow:
        row = cur.execute(_LOCK_LEASE, {"id": job_id, "token": token}).fetchone()
        if row is None:
            raise LeaseLost(f"job {job_id}: lease no longer valid")
        return _LeaseRow(**row)

    def _after_failure(self, lease: _LeaseRow, *, permanent: bool) -> tuple[JobState, float | None]:
        if lease.cancel_requested:
            return JobState.CANCELLED, None
        if permanent or lease.attempts >= lease.max_attempts:
            return JobState.DEAD, None
        delay = full_jitter_delay(
            lease.attempts, lease.backoff_base_seconds, lease.backoff_cap_seconds, self._rng
        )
        return JobState.FAILED, delay

    def _end_attempt(
        self,
        cur: psycopg.Cursor[DictRow],
        lease: _LeaseRow,
        state: JobState,
        outcome: str,
        error: str | None,
        delay: float | None,
    ) -> None:
        error = _truncate(error)
        cur.execute(
            _END_ATTEMPT_JOB,
            {
                "id": lease.id,
                "state": state.value,
                "delay": delay,
                "error": error,
                "terminal": state in (JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED),
            },
        )
        cur.execute(
            _END_ATTEMPT_HISTORY,
            {"id": lease.id, "attempt": lease.attempts, "outcome": outcome, "error": error},
        )
