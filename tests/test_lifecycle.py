"""JobQueue state changes: claim, complete, fail, backoff, dead-letter, fencing, reaper, cancel."""

from uuid import UUID, uuid4

import pytest
from helpers import expire_lease, make_due, sql

from queueforge import (
    InvalidTransition,
    Job,
    JobNotFound,
    JobQueue,
    JobState,
    LeaseLost,
    RetryPolicy,
    backoff_ceiling,
)
from queueforge.queue import MAX_ERROR_LENGTH

NO_DELAY = RetryPolicy(max_attempts=3, base_seconds=0, cap_seconds=0)


def claim_one(queue: JobQueue, worker: str = "w") -> tuple[Job, UUID]:
    jobs = queue.claim(worker)
    assert len(jobs) == 1
    assert jobs[0].lease_token is not None
    return jobs[0], jobs[0].lease_token


def outcomes(queue: JobQueue, job_id: int) -> list[str | None]:
    return [a.outcome for a in queue.attempts(job_id)]


def test_claim_leases_the_job_and_records_the_attempt(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    job, token = claim_one(queue, "worker-1")
    assert job.id == job_id
    assert job.state is JobState.RUNNING
    assert job.attempts == 1
    assert job.locked_by == "worker-1"
    assert job.lease_expires_at is not None
    [attempt] = queue.attempts(job_id)
    assert (attempt.attempt, attempt.worker_id, attempt.lease_token) == (1, "worker-1", token)
    assert attempt.outcome is None


def test_claim_on_an_empty_queue_returns_nothing(queue: JobQueue) -> None:
    assert queue.claim("w") == []


def test_claim_validates_arguments(queue: JobQueue) -> None:
    with pytest.raises(ValueError, match="limit"):
        queue.claim("w", limit=0)
    with pytest.raises(ValueError, match="lease"):
        queue.claim("w", lease_seconds=0)


def test_claim_respects_the_batch_limit(queue: JobQueue) -> None:
    for _ in range(5):
        queue.enqueue("t")
    assert len(queue.claim("w", limit=3)) == 3
    assert len(queue.claim("w", limit=3)) == 2


def test_complete_marks_success(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.complete(job_id, token)
    job = queue.get_job(job_id)
    assert job.state is JobState.SUCCEEDED
    assert job.finished_at is not None
    assert job.lease_token is None
    assert outcomes(queue, job_id) == ["succeeded"]


def test_completing_twice_is_rejected(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.complete(job_id, token)
    with pytest.raises(LeaseLost):
        queue.complete(job_id, token)


def test_completion_with_a_wrong_token_is_rejected(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    claim_one(queue)
    with pytest.raises(LeaseLost):
        queue.complete(job_id, uuid4())
    assert queue.get_job(job_id).state is JobState.RUNNING


def test_completion_after_lease_expiry_is_rejected_even_before_reaping(
    dsn: str, queue: JobQueue
) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    expire_lease(dsn, job_id)
    with pytest.raises(LeaseLost):
        queue.complete(job_id, token)
    with pytest.raises(LeaseLost):
        queue.fail(job_id, token, "boom")
    assert queue.heartbeat([(job_id, token)], lease_seconds=30) == []
    assert queue.get_job(job_id).state is JobState.RUNNING


def test_fencing_rejects_the_stale_worker_after_another_worker_reclaimed(
    dsn: str, queue: JobQueue
) -> None:
    job_id = queue.enqueue("t", retry=NO_DELAY).job_id
    _, stale_token = claim_one(queue, "worker-a")
    expire_lease(dsn, job_id)
    assert queue.reap_expired() == [(job_id, JobState.FAILED)]

    job, fresh_token = claim_one(queue, "worker-b")
    assert job.attempts == 2
    assert fresh_token != stale_token

    # worker-a wakes up (GC pause, network partition...) and tries to report.
    for stale_call in (
        lambda: queue.complete(job_id, stale_token),
        lambda: queue.fail(job_id, stale_token, "late failure"),
        lambda: queue.release(job_id, stale_token),
    ):
        with pytest.raises(LeaseLost):
            stale_call()
    assert queue.heartbeat([(job_id, stale_token)], lease_seconds=30) == []

    queue.complete(job_id, fresh_token)
    assert queue.get_job(job_id).state is JobState.SUCCEEDED
    history = queue.attempts(job_id)
    assert [(a.worker_id, a.outcome) for a in history] == [
        ("worker-a", "lease_expired"),
        ("worker-b", "succeeded"),
    ]


def test_heartbeat_extends_only_valid_leases(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    job, token = claim_one(queue)
    [result] = queue.heartbeat([(job_id, token), (job_id, uuid4())], lease_seconds=600)
    assert result.job_id == job_id and not result.cancel_requested
    renewed = queue.get_job(job_id)
    assert renewed.lease_expires_at is not None and job.lease_expires_at is not None
    assert renewed.lease_expires_at > job.lease_expires_at
    assert queue.heartbeat([], lease_seconds=1) == []


def test_failure_schedules_a_retry_with_backoff(dsn: str, queue: JobQueue) -> None:
    """With rng fixed at 1.0 each delay equals its ceiling: 1, 2, 4, 5 (cap), then dead."""
    policy = RetryPolicy(max_attempts=5, base_seconds=1.0, cap_seconds=5.0)
    job_id = queue.enqueue("t", retry=policy).job_id
    delays = []
    for attempt in range(1, 6):
        if attempt > 1:
            make_due(dsn, job_id)
        _, token = claim_one(queue)
        state = queue.fail(job_id, token, f"error {attempt}")
        if attempt < 5:
            assert state is JobState.FAILED
            # run_at and the attempt's finished_at come from the same transaction's now().
            [(delay,)] = sql(
                dsn,
                "SELECT extract(epoch FROM j.run_at - a.finished_at)::float8"
                " FROM queueforge_jobs j JOIN queueforge_attempts a ON a.job_id = j.id"
                " WHERE j.id = %s AND a.attempt = %s",
                (job_id, attempt),
            )
            delays.append(round(delay, 6))
        else:
            assert state is JobState.DEAD
    assert delays == [1.0, 2.0, 4.0, 5.0]
    assert delays == [backoff_ceiling(n, 1.0, 5.0) for n in range(1, 5)]


def test_jittered_delays_stay_inside_the_backoff_window(dsn: str, jittered_queue: JobQueue) -> None:
    queue = jittered_queue
    policy = RetryPolicy(max_attempts=50, base_seconds=10.0, cap_seconds=40.0)
    ids = [queue.enqueue("t", {"i": i}, retry=policy).job_id for i in range(30)]
    for _ in ids:
        job, token = claim_one(queue)
        queue.fail(job.id, token, "boom")
    rows = sql(
        dsn,
        "SELECT extract(epoch FROM j.run_at - a.finished_at)::float8"
        " FROM queueforge_jobs j JOIN queueforge_attempts a ON a.job_id = j.id"
        " WHERE j.id = ANY(%s)",
        (ids,),
    )
    delays = [d for (d,) in rows]
    assert all(0 <= d < 10.0 for d in delays)
    assert len(set(delays)) > 1  # actually jittered


def test_dead_letter_after_max_attempts_keeps_the_last_error(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t", retry=NO_DELAY).job_id
    for attempt in range(1, 4):
        _, token = claim_one(queue)
        state = queue.fail(job_id, token, f"error on attempt {attempt}")
    assert state is JobState.DEAD
    job = queue.get_job(job_id)
    assert job.state is JobState.DEAD
    assert job.last_error == "error on attempt 3"
    assert job.finished_at is not None
    assert outcomes(queue, job_id) == ["failed", "failed", "failed"]
    assert queue.claim("w") == []


def test_permanent_failure_dead_letters_immediately(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    assert queue.fail(job_id, token, "bad payload", permanent=True) is JobState.DEAD
    assert queue.get_job(job_id).attempts == 1


def test_long_errors_are_truncated(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.fail(job_id, token, "x" * (MAX_ERROR_LENGTH * 3), permanent=True)
    error = queue.get_job(job_id).last_error
    assert error is not None
    assert len(error) == MAX_ERROR_LENGTH and error.endswith("[truncated]")


def test_reaper_settles_expired_leases(dsn: str, queue: JobQueue) -> None:
    retry_id = queue.enqueue("t", retry=NO_DELAY).job_id
    last_try_id = queue.enqueue("t", retry=RetryPolicy(max_attempts=1)).job_id
    cancel_id = queue.enqueue("t").job_id
    healthy_id = queue.enqueue("t").job_id
    claimed = {job.id: job for job in queue.claim("crashed-worker", limit=4)}
    assert set(claimed) == {retry_id, last_try_id, cancel_id, healthy_id}
    queue.cancel(cancel_id)
    for job_id in (retry_id, last_try_id, cancel_id):
        expire_lease(dsn, job_id)

    settled = dict(queue.reap_expired())
    assert settled == {
        retry_id: JobState.FAILED,
        last_try_id: JobState.DEAD,
        cancel_id: JobState.CANCELLED,
    }
    assert queue.get_job(healthy_id).state is JobState.RUNNING
    assert outcomes(queue, retry_id) == ["lease_expired"]
    assert queue.get_job(retry_id).last_error == "lease expired (worker crashed-worker)"
    assert queue.reap_expired() == []


def test_reaper_only_touches_its_own_queue(dsn: str, pool: object, queue: JobQueue) -> None:
    other = JobQueue(pool, queue.queue + "-other")  # type: ignore[arg-type]
    job_id = other.enqueue("t").job_id
    other.claim("w")
    expire_lease(dsn, job_id)
    assert queue.reap_expired() == []
    assert other.reap_expired() == [(job_id, JobState.FAILED)]


def test_cancel_queued_and_retry_waiting_jobs(queue: JobQueue) -> None:
    queued_id = queue.enqueue("t").job_id
    assert queue.cancel(queued_id) is JobState.CANCELLED

    waiting_id = queue.enqueue("t", retry=RetryPolicy(3, 60, 60)).job_id
    _, token = claim_one(queue)
    assert queue.fail(waiting_id, token, "boom") is JobState.FAILED
    assert queue.cancel(waiting_id) is JobState.CANCELLED
    assert queue.claim("w") == []
    assert queue.get_job(waiting_id).finished_at is not None


def test_cancel_running_job_is_cooperative(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    assert queue.cancel(job_id) is JobState.RUNNING
    [beat] = queue.heartbeat([(job_id, token)], lease_seconds=30)
    assert beat.cancel_requested
    queue.acknowledge_cancel(job_id, token)
    assert queue.get_job(job_id).state is JobState.CANCELLED
    assert outcomes(queue, job_id) == ["cancelled"]


def test_failure_after_cancel_request_does_not_retry(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.cancel(job_id)
    assert queue.fail(job_id, token, "interrupted") is JobState.CANCELLED


def test_acknowledge_cancel_requires_a_request(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    with pytest.raises(InvalidTransition):
        queue.acknowledge_cancel(job_id, token)


def test_cancel_of_finished_or_missing_jobs(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.complete(job_id, token)
    assert queue.cancel(job_id) is JobState.SUCCEEDED
    with pytest.raises(JobNotFound):
        queue.cancel(-1)


def test_release_requeues_without_resetting_the_attempt_counter(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    assert queue.release(job_id, token) is JobState.QUEUED
    job = queue.get_job(job_id)
    assert job.state is JobState.QUEUED and job.attempts == 1
    again, _ = claim_one(queue)
    assert again.attempts == 2
    assert outcomes(queue, job_id) == ["released", None]


def test_release_of_a_cancel_requested_job_cancels_it(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    _, token = claim_one(queue)
    queue.cancel(job_id)
    assert queue.release(job_id, token) is JobState.CANCELLED


def test_requeue_dead_job(queue: JobQueue) -> None:
    job_id = queue.enqueue("t", retry=RetryPolicy(max_attempts=1)).job_id
    _, token = claim_one(queue)
    queue.fail(job_id, token, "boom")
    queue.requeue_dead(job_id, extra_attempts=2)
    job = queue.get_job(job_id)
    assert (job.state, job.max_attempts, job.finished_at) == (JobState.QUEUED, 3, None)
    again, _ = claim_one(queue)
    assert again.attempts == 2


def test_requeue_only_applies_to_dead_jobs(queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    with pytest.raises(InvalidTransition):
        queue.requeue_dead(job_id)
    with pytest.raises(ValueError, match="extra_attempts"):
        queue.requeue_dead(job_id, extra_attempts=0)


def test_get_job_missing(queue: JobQueue) -> None:
    with pytest.raises(JobNotFound):
        queue.get_job(-1)


def test_stats_reports_counts_age_and_expired_leases(dsn: str, queue: JobQueue) -> None:
    empty = queue.stats()
    assert set(empty.counts.values()) == {0}
    assert empty.oldest_runnable_age_seconds is None

    queue.enqueue("t")
    queue.enqueue("t")
    running_id = queue.enqueue("t", priority=10).job_id
    queue.enqueue("t", delay_seconds=3600)  # scheduled: not runnable, not counted in the age
    claim_one(queue)
    sql(
        dsn,
        "UPDATE queueforge_jobs SET run_at = now() - interval '90 seconds'"
        " WHERE queue = %s AND state = 'queued' AND run_at <= now()",
        (queue.queue,),
    )
    expire_lease(dsn, running_id)

    stats = queue.stats()
    assert stats.counts["queued"] == 3
    assert stats.counts["running"] == 1
    assert stats.expired_leases == 1
    assert stats.oldest_runnable_age_seconds is not None
    assert 89 < stats.oldest_runnable_age_seconds < 120
    assert stats.as_dict()["queue"] == queue.queue
