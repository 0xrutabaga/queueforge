"""In-process Worker behaviour: retries, dead-letter, cancellation, shutdown, timeouts, outages."""

import threading
import time
from typing import Any

import psycopg
import pytest
from helpers import expire_lease, running, wait_for

from queueforge import (
    JobContext,
    JobQueue,
    JobState,
    PermanentError,
    Registry,
    RetryPolicy,
    StopReason,
    Worker,
)

NO_DELAY = RetryPolicy(max_attempts=3, base_seconds=0, cap_seconds=0)


def make_worker(queue: JobQueue, registry: Registry, **kwargs: Any) -> Worker:
    defaults: dict[str, Any] = {
        "concurrency": 4,
        "lease_seconds": 5.0,
        "poll_seconds": 0.02,
        "shutdown_grace_seconds": 2.0,
    }
    return Worker(queue, registry, **{**defaults, **kwargs})


def state_is(queue: JobQueue, job_id: int, state: JobState) -> bool:
    return queue.get_job(job_id).state is state


def test_worker_runs_jobs_to_success(queue: JobQueue) -> None:
    registry = Registry()
    seen: list[Any] = []

    @registry.task("record")
    def record(ctx: JobContext, payload: Any) -> None:
        seen.append(payload["n"])

    ids = [queue.enqueue("record", {"n": n}).job_id for n in range(20)]
    with running(make_worker(queue, registry)):
        wait_for(lambda: queue.stats().counts["succeeded"] == 20, what="20 successes")
    assert sorted(seen) == list(range(20))
    assert all(queue.get_job(i).attempts == 1 for i in ids)


def test_worker_retries_until_success(queue: JobQueue) -> None:
    registry = Registry()

    @registry.task("flaky")
    def flaky(ctx: JobContext, payload: Any) -> None:
        if ctx.attempt < 3:
            raise RuntimeError(f"transient failure {ctx.attempt}")

    job_id = queue.enqueue("flaky", retry=NO_DELAY).job_id
    with running(make_worker(queue, registry)):
        wait_for(lambda: state_is(queue, job_id, JobState.SUCCEEDED))
    assert [a.outcome for a in queue.attempts(job_id)] == ["failed", "failed", "succeeded"]
    assert "transient failure 2" in (queue.attempts(job_id)[1].error or "")


def test_worker_dead_letters_a_poison_job(queue: JobQueue) -> None:
    registry = Registry()

    @registry.task("poison")
    def poison(ctx: JobContext, payload: Any) -> None:
        raise ValueError("always broken")

    job_id = queue.enqueue("poison", retry=NO_DELAY).job_id
    with running(make_worker(queue, registry)):
        wait_for(lambda: state_is(queue, job_id, JobState.DEAD))
    job = queue.get_job(job_id)
    assert job.attempts == 3
    assert job.last_error is not None and "ValueError: always broken" in job.last_error


def test_permanent_error_skips_retries(queue: JobQueue) -> None:
    registry = Registry()

    @registry.task("invalid")
    def invalid(ctx: JobContext, payload: Any) -> None:
        raise PermanentError("bad input")

    job_id = queue.enqueue("invalid", retry=NO_DELAY).job_id
    with running(make_worker(queue, registry)):
        wait_for(lambda: state_is(queue, job_id, JobState.DEAD))
    assert queue.get_job(job_id).attempts == 1


def test_unknown_task_is_dead_lettered(queue: JobQueue) -> None:
    job_id = queue.enqueue("nobody_handles_this").job_id
    with running(make_worker(queue, Registry())):
        wait_for(lambda: state_is(queue, job_id, JobState.DEAD))
    assert "no handler registered" in (queue.get_job(job_id).last_error or "")


def test_running_job_is_cancelled_cooperatively(queue: JobQueue) -> None:
    registry = Registry()
    started = threading.Event()

    @registry.task("long")
    def long(ctx: JobContext, payload: Any) -> None:
        started.set()
        for _ in range(500):
            ctx.sleep(0.02)

    job_id = queue.enqueue("long").job_id
    with running(make_worker(queue, registry, lease_seconds=1.0, heartbeat_seconds=0.1)):
        assert started.wait(10)
        assert queue.cancel(job_id) is JobState.RUNNING
        wait_for(lambda: state_is(queue, job_id, JobState.CANCELLED))
    assert [a.outcome for a in queue.attempts(job_id)] == ["cancelled"]


def test_graceful_shutdown_releases_cooperative_jobs(queue: JobQueue) -> None:
    registry = Registry()
    started = threading.Event()

    @registry.task("long")
    def long(ctx: JobContext, payload: Any) -> None:
        started.set()
        while True:
            ctx.sleep(0.02)

    job_id = queue.enqueue("long").job_id
    worker = make_worker(queue, registry)
    with running(worker):
        assert started.wait(10)
    job = queue.get_job(job_id)
    assert job.state is JobState.QUEUED
    assert [a.outcome for a in queue.attempts(job_id)] == ["released"]


def test_shutdown_leaves_uncooperative_jobs_to_lease_expiry(queue: JobQueue) -> None:
    """A handler that ignores the stop request is not released: releasing it would let another
    worker start the job while this thread still runs. Its late completion is fenced off."""
    registry = Registry()
    started = threading.Event()
    finished = threading.Event()

    @registry.task("stubborn")
    def stubborn(ctx: JobContext, payload: Any) -> None:
        started.set()
        time.sleep(1.5)  # ignores ctx entirely
        finished.set()

    job_id = queue.enqueue("stubborn", retry=NO_DELAY).job_id
    worker = make_worker(queue, registry, lease_seconds=0.6, shutdown_grace_seconds=0.1)
    result: dict[str, Any] = {}
    thread = threading.Thread(target=lambda: result.update(report=worker.run()))
    thread.start()
    assert started.wait(10)
    worker.stop()
    thread.join(10)
    assert result["report"].abandoned_jobs == [job_id]
    assert queue.get_job(job_id).state is JobState.RUNNING

    assert finished.wait(10)
    time.sleep(0.2)  # the zombie thread's complete() call is rejected: lease expired
    assert queue.get_job(job_id).state is JobState.RUNNING
    assert queue.reap_expired() == [(job_id, JobState.FAILED)]


def test_job_timeout_fails_the_attempt(queue: JobQueue) -> None:
    registry = Registry()
    reasons: list[frozenset[StopReason]] = []

    @registry.task("slow")
    def slow(ctx: JobContext, payload: Any) -> None:
        try:
            while True:
                ctx.sleep(0.02)
        finally:
            reasons.append(ctx.stop_reasons)

    job_id = queue.enqueue("slow", retry=RetryPolicy(max_attempts=1)).job_id
    worker = make_worker(
        queue, registry, lease_seconds=2.0, heartbeat_seconds=0.05, job_timeout_seconds=0.3
    )
    with running(worker):
        wait_for(lambda: state_is(queue, job_id, JobState.DEAD))
    assert "timed out" in (queue.get_job(job_id).last_error or "")
    assert reasons == [frozenset({StopReason.TIMEOUT})]


def test_handler_is_told_when_its_lease_is_lost(dsn: str, queue: JobQueue) -> None:
    registry = Registry()
    started = threading.Event()
    reasons: list[frozenset[StopReason]] = []

    @registry.task("long")
    def long(ctx: JobContext, payload: Any) -> None:
        started.set()
        try:
            while True:
                ctx.sleep(0.02)
        finally:
            reasons.append(ctx.stop_reasons)

    job_id = queue.enqueue("long", retry=NO_DELAY).job_id
    worker = make_worker(
        queue, registry, lease_seconds=30.0, heartbeat_seconds=0.05, reap_seconds=3600
    )
    with running(worker):
        assert started.wait(10)
        expire_lease(dsn, job_id)  # as if the worker had stalled past its lease
        wait_for(lambda: bool(reasons), what="handler to stop")
    assert reasons == [frozenset({StopReason.LEASE_LOST})]
    assert queue.reap_expired() == [(job_id, JobState.FAILED)]


def test_worker_survives_a_database_outage(queue_name: str) -> None:
    """Claims against an unreachable database are logged and retried, never fatal."""
    unreachable = JobQueue.connect(
        "postgresql://nobody@127.0.0.1:1/nothing?connect_timeout=1",
        queue_name,
        pool_timeout=0.2,
    )
    try:
        worker = make_worker(unreachable, Registry(), error_backoff_seconds=0.05)
        with running(worker):
            time.sleep(1.0)
        assert worker.inflight() == []
    finally:
        unreachable.close()


def test_registry_rejects_duplicates_and_empty_names() -> None:
    registry = Registry()
    registry.register("a", lambda ctx, payload: None)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("a", lambda ctx, payload: None)
    with pytest.raises(ValueError, match="empty"):
        registry.register("", lambda ctx, payload: None)
    assert registry.names() == ["a"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"concurrency": 0}, "concurrency"),
        ({"lease_seconds": 0}, "lease_seconds"),
        ({"lease_seconds": 1, "heartbeat_seconds": 1}, "heartbeat"),
        ({"job_timeout_seconds": 0}, "job_timeout"),
    ],
)
def test_worker_validates_settings(queue: JobQueue, kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Worker(queue, Registry(), **kwargs)


def test_handler_is_stopped_when_heartbeats_keep_failing(
    queue: JobQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the worker cannot reach the database for a whole lease period, another worker may
    already own the job, so the handler is told to stop even though no heartbeat said so."""
    registry = Registry()
    started = threading.Event()
    reasons: list[frozenset[StopReason]] = []

    @registry.task("long")
    def long(ctx: JobContext, payload: Any) -> None:
        started.set()
        try:
            while True:
                ctx.sleep(0.02)
        finally:
            reasons.append(ctx.stop_reasons)

    def unreachable(*args: Any, **kwargs: Any) -> Any:
        raise psycopg.OperationalError("simulated network partition")

    queue.enqueue("long", retry=NO_DELAY)
    worker = make_worker(queue, registry, lease_seconds=0.6, heartbeat_seconds=0.1)
    monkeypatch.setattr(worker.queue, "heartbeat", unreachable)
    with running(worker):
        assert started.wait(10)
        began = time.monotonic()
        wait_for(lambda: bool(reasons), timeout=5, what="handler to stop")
        assert time.monotonic() - began < 1.0
    assert reasons == [frozenset({StopReason.LEASE_LOST})]
