"""No job is ever executed by two workers at the same time.

Three levels: raw claims from many threads, many in-process workers with injected failures, and
several worker processes checked with a PostgreSQL advisory lock per job.
"""

import random
import threading
import time
from collections import Counter
from itertools import pairwise
from pathlib import Path
from typing import Any

from helpers import running, spawn_worker, sql, wait_for

from queueforge import JobContext, JobQueue, JobState, Registry, RetryPolicy, Worker

RETRY_NOW = RetryPolicy(max_attempts=25, base_seconds=0, cap_seconds=0)


def assert_attempts_never_overlap(dsn: str, queue: str) -> None:
    rows = sql(
        dsn,
        "SELECT a.job_id, a.attempt, a.started_at, a.finished_at, a.outcome"
        " FROM queueforge_attempts a JOIN queueforge_jobs j ON j.id = a.job_id"
        " WHERE j.queue = %s ORDER BY a.job_id, a.attempt",
        (queue,),
    )
    by_job: dict[int, list[tuple[Any, ...]]] = {}
    for row in rows:
        by_job.setdefault(row[0], []).append(row)
    for attempts in by_job.values():
        assert [a[1] for a in attempts] == list(range(1, len(attempts) + 1))
        for earlier, later in pairwise(attempts):
            assert earlier[3] is not None and later[2] >= earlier[3]
        assert [a[4] for a in attempts].count("succeeded") <= 1


def test_concurrent_claims_never_hand_out_a_job_twice(queue: JobQueue) -> None:
    total = 300
    for i in range(total):
        queue.enqueue("t", {"i": i})
    claimed: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(12)

    def claimer(n: int) -> None:
        barrier.wait()
        while True:
            jobs = queue.claim(f"claimer-{n}", limit=random.randint(1, 5))
            if not jobs:
                return
            with lock:
                claimed.extend(job.id for job in jobs)

    threads = [threading.Thread(target=claimer, args=(n,)) for n in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == total
    assert len(set(claimed)) == total


def test_many_workers_with_random_failures_never_overlap(dsn: str, queue: JobQueue) -> None:
    total = 400
    registry = Registry()
    active: set[int] = set()
    executions: Counter[int] = Counter()
    violations: list[int] = []
    lock = threading.Lock()

    @registry.task("work")
    def work(ctx: JobContext, payload: Any) -> None:
        with lock:
            if ctx.job_id in active:
                violations.append(ctx.job_id)
            active.add(ctx.job_id)
            executions[ctx.job_id] += 1
        try:
            time.sleep(random.uniform(0, 0.003))
            if random.random() < 0.2:
                raise RuntimeError("injected failure")
        finally:
            with lock:
                active.discard(ctx.job_id)

    ids = [queue.enqueue("work", {"i": i}, retry=RETRY_NOW).job_id for i in range(total)]
    workers = [
        Worker(
            queue,
            registry,
            worker_id=f"w{n}",
            concurrency=4,
            lease_seconds=30,
            poll_seconds=0.01,
        )
        for n in range(4)
    ]
    with running(workers[0]), running(workers[1]), running(workers[2]), running(workers[3]):
        wait_for(
            lambda: queue.stats().counts["succeeded"] == total, timeout=90, what="all successes"
        )

    assert violations == []
    jobs = {i: queue.get_job(i) for i in ids}
    assert all(job.state is JobState.SUCCEEDED for job in jobs.values())
    # Every execution is one attempt and vice versa: no job ran without holding a lease.
    assert {i: job.attempts for i, job in jobs.items()} == dict(executions)
    assert sum(executions.values()) > total  # failures really happened and were retried
    assert_attempts_never_overlap(dsn, queue.queue)


def test_worker_processes_never_run_a_job_concurrently(
    dsn: str, queue: JobQueue, tmp_path: Path
) -> None:
    total = 200
    ids = [
        queue.enqueue("probe", {"i": i, "fail_rate": 0.2}, retry=RETRY_NOW).job_id
        for i in range(total)
    ]
    procs = [
        spawn_worker(
            dsn,
            queue.queue,
            f"proc-{n}",
            tmp_path / f"proc-{n}.log",
            "--concurrency",
            "4",
            "--lease-seconds",
            "30",
        )
        for n in range(3)
    ]
    try:
        wait_for(
            lambda: queue.stats().counts["succeeded"] == total, timeout=120, what="all successes"
        )
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()

    assert sql(dsn, "SELECT * FROM qf_test_violations WHERE job_id = ANY(%s)", (ids,)) == []
    executions = dict(
        sql(
            dsn,
            "SELECT job_id, count(*) FROM qf_test_executions WHERE job_id = ANY(%s)"
            " GROUP BY job_id",
            (ids,),
        )
    )
    assert executions == {i: queue.get_job(i).attempts for i in ids}
    workers_used = sql(
        dsn,
        "SELECT DISTINCT worker FROM qf_test_executions WHERE job_id = ANY(%s)",
        (ids,),
    )
    assert len(workers_used) > 1
    assert_attempts_never_overlap(dsn, queue.queue)
