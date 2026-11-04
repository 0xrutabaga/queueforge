import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from helpers import make_due

from queueforge import IdempotencyConflict, JobNotFound, JobQueue, JobState, RetryPolicy


def test_enqueue_defaults(queue: JobQueue) -> None:
    result = queue.enqueue("send_email", {"to": "a@example.com"})
    assert result.created
    job = queue.get_job(result.job_id)
    assert job.state is JobState.QUEUED
    assert job.payload == {"to": "a@example.com"}
    assert job.attempts == 0
    assert job.max_attempts == 5
    assert job.lease_token is None


def test_same_idempotency_key_returns_the_existing_job(queue: JobQueue) -> None:
    first = queue.enqueue("charge", {"order": 7}, idempotency_key="order-7")
    second = queue.enqueue("charge", {"order": 7}, idempotency_key="order-7")
    assert first.created and not second.created
    assert first.job_id == second.job_id
    assert queue.stats().counts["queued"] == 1


def test_idempotent_enqueue_still_dedupes_after_the_job_finished(queue: JobQueue) -> None:
    first = queue.enqueue("charge", {"order": 8}, idempotency_key="order-8")
    job = queue.claim("w")[0]
    assert job.lease_token is not None
    queue.complete(job.id, job.lease_token)
    again = queue.enqueue("charge", {"order": 8}, idempotency_key="order-8")
    assert again.job_id == first.job_id and not again.created


@pytest.mark.parametrize(("task", "payload"), [("charge", {"order": 2}), ("refund", {"order": 1})])
def test_reusing_a_key_with_different_content_is_a_conflict(
    queue: JobQueue, task: str, payload: dict[str, int]
) -> None:
    queue.enqueue("charge", {"order": 1}, idempotency_key="k")
    with pytest.raises(IdempotencyConflict):
        queue.enqueue(task, payload, idempotency_key="k")


def test_idempotency_keys_are_scoped_per_queue(pool: Any, queue_name: str) -> None:
    a = JobQueue(pool, queue_name + "-a").enqueue("t", idempotency_key="k")
    b = JobQueue(pool, queue_name + "-b").enqueue("t", idempotency_key="k")
    assert a.created and b.created and a.job_id != b.job_id


def test_concurrent_enqueues_with_one_key_create_one_job(queue: JobQueue) -> None:
    barrier = threading.Barrier(16)
    results = []
    lock = threading.Lock()

    def producer() -> None:
        barrier.wait()
        result = queue.enqueue("t", {"x": 1}, idempotency_key="race")
        with lock:
            results.append(result)

    threads = [threading.Thread(target=producer) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 16
    assert len({r.job_id for r in results}) == 1
    assert sum(r.created for r in results) == 1
    assert queue.stats().counts["queued"] == 1


def test_enqueue_inside_a_rolled_back_transaction_leaves_no_job(dsn: str, queue: JobQueue) -> None:
    with psycopg.connect(dsn) as conn:
        job_id = queue.enqueue("t", conn=conn).job_id
        conn.rollback()
    with pytest.raises(JobNotFound):
        queue.get_job(job_id)


def test_enqueue_inside_a_committed_transaction(dsn: str, queue: JobQueue) -> None:
    with psycopg.connect(dsn) as conn:
        job_id = queue.enqueue("t", conn=conn).job_id
        conn.commit()
    assert queue.get_job(job_id).state is JobState.QUEUED


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"task": ""}, "task"),
        ({"task": "t", "delay_seconds": -1}, "delay"),
        ({"task": "t", "run_at": datetime(2030, 1, 1)}, "timezone"),
        ({"task": "t", "idempotency_key": ""}, "idempotency_key"),
    ],
)
def test_enqueue_rejects_bad_input(queue: JobQueue, kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        queue.enqueue(**kwargs)


def test_scheduled_job_is_not_claimed_before_run_at(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t", delay_seconds=3600).job_id
    assert queue.claim("w") == []
    make_due(dsn, job_id)
    assert [j.id for j in queue.claim("w")] == [job_id]


def test_explicit_run_at_in_the_past_is_runnable(queue: JobQueue) -> None:
    job_id = queue.enqueue("t", run_at=datetime.now(UTC) - timedelta(minutes=1)).job_id
    assert [j.id for j in queue.claim("w")] == [job_id]


def test_claims_follow_priority_then_fifo(queue: JobQueue) -> None:
    low = queue.enqueue("t", priority=0).job_id
    high = queue.enqueue("t", priority=10).job_id
    mid_1 = queue.enqueue("t", priority=5).job_id
    mid_2 = queue.enqueue("t", priority=5).job_id
    order = [queue.claim("w")[0].id for _ in range(4)]
    assert order == [high, mid_1, mid_2, low]


def test_retry_policy_is_stored_on_the_job(queue: JobQueue) -> None:
    job_id = queue.enqueue("t", retry=RetryPolicy(3, 0.5, 4.0)).job_id
    job = queue.get_job(job_id)
    assert (job.max_attempts, job.backoff_base_seconds, job.backoff_cap_seconds) == (3, 0.5, 4.0)


def test_repeatable_read_caller_sees_a_serialization_failure_on_a_concurrent_key(
    dsn: str, queue: JobQueue
) -> None:
    """In REPEATABLE READ or SERIALIZABLE, a key committed by another transaction after the
    caller's snapshot is a serialization failure (SQLSTATE 40001): retry the transaction."""
    with psycopg.connect(dsn) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")  # take the snapshot
        other = queue.enqueue("t", {"a": 1}, idempotency_key="rr")
        with pytest.raises(psycopg.errors.SerializationFailure):
            queue.enqueue("t", {"a": 1}, idempotency_key="rr", conn=conn)
        conn.rollback()
        again = queue.enqueue("t", {"a": 1}, idempotency_key="rr", conn=conn)  # the retry
        conn.commit()
    assert again.job_id == other.job_id and not again.created
