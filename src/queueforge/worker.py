"""Worker: claims jobs, runs handlers on threads, heartbeats leases, reaps expired leases.

Delivery is at-least-once. A job's handler can run more than once (after a crash, a lost lease
or a timeout), so handlers must be idempotent or use ``JobContext.fencing_token`` with the
systems they write to. Only the attempt that still holds a valid lease can change the job's
state; stale attempts get :class:`queueforge.errors.LeaseLost` and their result is discarded.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar
from uuid import UUID

import psycopg

from queueforge.errors import JobInterrupted, LeaseLost, PermanentError
from queueforge.models import Job
from queueforge.queue import JobQueue

log = logging.getLogger("queueforge.worker")

Handler = Callable[["JobContext", Any], object]
H = TypeVar("H", bound=Handler)


class StopReason(StrEnum):
    CANCELLED = "cancelled"  # an operator cancelled the job
    SHUTDOWN = "shutdown"  # the worker is stopping
    TIMEOUT = "timeout"  # the handler exceeded the worker's job timeout
    LEASE_LOST = "lease_lost"  # a heartbeat found the lease gone; results will be rejected


class JobContext:
    """Handed to every handler call. Handlers should poll it during long work."""

    def __init__(self, job: Job, lease_deadline: float | None = None) -> None:
        if job.lease_token is None:
            raise ValueError("JobContext needs a leased job")
        self.job = job
        self.lease_token: UUID = job.lease_token
        self.started_monotonic = time.monotonic()
        # Local monotonic time by which the lease has certainly expired unless renewed. It is
        # measured from *before* the claim/heartbeat request, so it is never later than the
        # database's own lease_expires_at.
        self.lease_deadline = lease_deadline if lease_deadline is not None else float("inf")
        self._event = threading.Event()
        self._reasons: set[StopReason] = set()
        self._lock = threading.Lock()

    @property
    def job_id(self) -> int:
        return self.job.id

    @property
    def attempt(self) -> int:
        """1-based attempt number. Strictly increases with every claim of this job."""
        return self.job.attempts

    @property
    def fencing_token(self) -> tuple[int, int]:
        """(job id, attempt): pass to downstream systems that reject writes from older attempts."""
        return (self.job.id, self.job.attempts)

    @property
    def stop_reasons(self) -> frozenset[StopReason]:
        with self._lock:
            return frozenset(self._reasons)

    def stop_requested(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        """Raise JobInterrupted if the worker asked this handler to stop."""
        if self._event.is_set():
            raise JobInterrupted(", ".join(sorted(self.stop_reasons)))

    def sleep(self, seconds: float) -> None:
        """Sleep, but wake up and raise JobInterrupted as soon as a stop is requested."""
        if self._event.wait(seconds):
            self.check()

    def request_stop(self, reason: StopReason) -> None:
        with self._lock:
            self._reasons.add(reason)
        self._event.set()


class Registry:
    """Maps task names to handler functions ``handler(ctx, payload)``."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def task(self, name: str) -> Callable[[H], H]:
        def decorator(fn: H) -> H:
            self.register(name, fn)
            return fn

        return decorator

    def register(self, name: str, fn: Handler) -> None:
        if not name:
            raise ValueError("task name must not be empty")
        if name in self._handlers:
            raise ValueError(f"task {name!r} is already registered")
        self._handlers[name] = fn

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def names(self) -> list[str]:
        return sorted(self._handlers)


@dataclass(frozen=True, slots=True)
class WorkerReport:
    worker_id: str
    abandoned_jobs: list[int]  # still running after the shutdown grace period


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


def _describe(exc: BaseException) -> str:
    return "".join(traceback.format_exception(exc)).strip()


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        registry: Registry,
        *,
        worker_id: str | None = None,
        concurrency: int = 4,
        lease_seconds: float = 30.0,
        heartbeat_seconds: float | None = None,
        poll_seconds: float = 1.0,
        reap_seconds: float | None = None,
        job_timeout_seconds: float | None = None,
        shutdown_grace_seconds: float = 10.0,
        error_backoff_seconds: float = 1.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        heartbeat = heartbeat_seconds if heartbeat_seconds is not None else lease_seconds / 3
        if not 0 < heartbeat < lease_seconds:
            raise ValueError("heartbeat_seconds must be > 0 and < lease_seconds")
        if job_timeout_seconds is not None and job_timeout_seconds <= 0:
            raise ValueError("job_timeout_seconds must be > 0")
        self.queue = queue
        self.registry = registry
        self.worker_id = worker_id or default_worker_id()
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat
        self.poll_seconds = poll_seconds
        self.reap_seconds = reap_seconds if reap_seconds is not None else lease_seconds / 2
        self.job_timeout_seconds = job_timeout_seconds
        self.shutdown_grace_seconds = shutdown_grace_seconds
        self.error_backoff_seconds = error_backoff_seconds

        # Keyed by lease token, not job id: after a lost lease the same worker can claim the
        # next attempt of a job while the stale attempt's thread is still running.
        self._inflight: dict[UUID, JobContext] = {}
        self._lock = threading.Lock()
        self._stopping = threading.Event()
        self._wake = threading.Event()
        self._heartbeat_stop = threading.Event()

    # ------------------------------------------------------------------ public

    def stop(self) -> None:
        """Ask the worker to stop claiming and shut down gracefully. Safe from any thread."""
        self._stopping.set()
        self._wake.set()

    def inflight(self) -> list[int]:
        """Job ids of running handler threads, one entry per thread (stale attempts included)."""
        with self._lock:
            return sorted(ctx.job_id for ctx in self._inflight.values())

    def run(self) -> WorkerReport:
        """Process jobs until stop() is called. Returns after the graceful shutdown."""
        log.info(
            "worker %s starting: queue=%s concurrency=%d lease=%.1fs",
            self.worker_id,
            self.queue.queue,
            self.concurrency,
            self.lease_seconds,
        )
        heartbeater = threading.Thread(
            target=self._heartbeat_loop, name=f"qf-heartbeat-{self.worker_id}", daemon=True
        )
        heartbeater.start()
        try:
            self._claim_loop()
        finally:
            abandoned = self._drain()
            self._heartbeat_stop.set()
            heartbeater.join(timeout=self.heartbeat_seconds + 5)
        log.info("worker %s stopped", self.worker_id)
        return WorkerReport(self.worker_id, abandoned)

    # ------------------------------------------------------------------ loops

    def _claim_loop(self) -> None:
        next_reap = 0.0
        while not self._stopping.is_set():
            self._wake.clear()
            if time.monotonic() >= next_reap:
                self._reap()
                next_reap = time.monotonic() + self.reap_seconds
            with self._lock:
                free = self.concurrency - len(self._inflight)
            if free > 0:
                requested_at = time.monotonic()
                try:
                    jobs = self.queue.claim(
                        self.worker_id, limit=free, lease_seconds=self.lease_seconds
                    )
                except psycopg.Error as exc:
                    log.warning(
                        "claim failed (%s); retrying in %.1fs", exc, self.error_backoff_seconds
                    )
                    self._stopping.wait(self.error_backoff_seconds)
                    continue
                for job in jobs:
                    self._start(job, requested_at + self.lease_seconds)
                if len(jobs) == free:
                    continue  # there may be more work and we may have capacity again soon
            self._wake.wait(self.poll_seconds)

    def _reap(self) -> None:
        try:
            for job_id, state in self.queue.reap_expired():
                log.warning("job %d: lease expired, now %s", job_id, state)
        except psycopg.Error as exc:
            log.warning("reap failed: %s", exc)

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            with self._lock:
                contexts = list(self._inflight.values())
            renew: list[JobContext] = []
            for ctx in contexts:
                elapsed = time.monotonic() - ctx.started_monotonic
                if self.job_timeout_seconds is not None and elapsed > self.job_timeout_seconds:
                    # Stop renewing: the lease runs out even if the handler ignores the request.
                    if StopReason.TIMEOUT not in ctx.stop_reasons:
                        log.warning("job %d: exceeded %.1fs timeout", ctx.job_id, elapsed)
                    ctx.request_stop(StopReason.TIMEOUT)
                elif StopReason.LEASE_LOST in ctx.stop_reasons:
                    continue
                elif time.monotonic() >= ctx.lease_deadline:
                    # Heartbeats have been failing (for example the database is unreachable)
                    # for a whole lease period: another worker may own the job now.
                    log.warning("job %d: lease could not be renewed in time", ctx.job_id)
                    ctx.request_stop(StopReason.LEASE_LOST)
                else:
                    renew.append(ctx)
            if not renew:
                continue
            requested_at = time.monotonic()
            try:
                results = self.queue.heartbeat(
                    [(ctx.job_id, ctx.lease_token) for ctx in renew],
                    lease_seconds=self.lease_seconds,
                )
            except psycopg.Error as exc:
                log.warning("heartbeat failed: %s", exc)
                continue
            alive = {result.job_id: result for result in results}
            for ctx in renew:
                result = alive.get(ctx.job_id)
                if result is None:
                    with self._lock:
                        still_running = ctx.lease_token in self._inflight
                    if still_running:
                        log.warning("job %d: lease lost", ctx.job_id)
                        ctx.request_stop(StopReason.LEASE_LOST)
                else:
                    ctx.lease_deadline = requested_at + self.lease_seconds
                    if result.cancel_requested:
                        ctx.request_stop(StopReason.CANCELLED)

    def _drain(self) -> list[int]:
        with self._lock:
            contexts = list(self._inflight.values())
        for ctx in contexts:
            ctx.request_stop(StopReason.SHUTDOWN)
        deadline = time.monotonic() + self.shutdown_grace_seconds
        while time.monotonic() < deadline:
            with self._lock:
                if not self._inflight:
                    return []
            self._wake.wait(min(0.05, max(0.0, deadline - time.monotonic())))
            self._wake.clear()
        abandoned = self.inflight()
        if abandoned:
            # Releasing these would let another worker start them while our thread still runs.
            # Leave the leases to expire instead; the reaper will retry them.
            log.warning(
                "worker %s: %d job(s) still running after %.1fs grace; leases left to expire: %s",
                self.worker_id,
                len(abandoned),
                self.shutdown_grace_seconds,
                abandoned,
            )
        return abandoned

    # ------------------------------------------------------------------ execution

    def _start(self, job: Job, lease_deadline: float) -> None:
        ctx = JobContext(job, lease_deadline)
        with self._lock:
            self._inflight[ctx.lease_token] = ctx
        thread = threading.Thread(
            target=self._execute, args=(ctx,), name=f"qf-job-{job.id}", daemon=True
        )
        thread.start()

    def _execute(self, ctx: JobContext) -> None:
        job = ctx.job
        try:
            handler = self.registry.get(job.task)
            if handler is None:
                self.queue.fail(
                    job.id,
                    ctx.lease_token,
                    f"no handler registered for task {job.task!r}",
                    permanent=True,
                )
                return
            try:
                handler(ctx, job.payload)
            except JobInterrupted:
                self._settle_interrupted(ctx)
            except PermanentError as exc:
                self.queue.fail(job.id, ctx.lease_token, _describe(exc), permanent=True)
            except Exception as exc:
                state = self.queue.fail(job.id, ctx.lease_token, _describe(exc))
                log.info("job %d attempt %d failed, now %s: %s", job.id, ctx.attempt, state, exc)
            else:
                self.queue.complete(job.id, ctx.lease_token)
        except LeaseLost:
            log.warning("job %d attempt %d: lease lost, result discarded", job.id, ctx.attempt)
        except psycopg.Error:
            log.exception(
                "job %d attempt %d: could not record the outcome; the lease will expire and "
                "the job will be retried",
                job.id,
                ctx.attempt,
            )
        finally:
            with self._lock:
                self._inflight.pop(ctx.lease_token, None)
            self._wake.set()

    def _settle_interrupted(self, ctx: JobContext) -> None:
        reasons = ctx.stop_reasons
        job_id, token = ctx.job_id, ctx.lease_token
        if StopReason.LEASE_LOST in reasons:
            log.warning("job %d attempt %d: stopped after losing its lease", job_id, ctx.attempt)
        elif StopReason.CANCELLED in reasons:
            self.queue.acknowledge_cancel(job_id, token)
            log.info("job %d: cancelled", job_id)
        elif StopReason.TIMEOUT in reasons:
            self.queue.fail(job_id, token, "timed out after the worker's job timeout")
        else:
            state = self.queue.release(job_id, token)
            log.info("job %d: released on shutdown, now %s", job_id, state)
