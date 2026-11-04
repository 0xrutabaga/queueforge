"""End-to-end demo: injected failures, a hard-killed worker, cancellation, dead-lettering.

Run with ``uv run queueforge demo`` after ``docker compose up -d --wait``. It uses the queue
named ``demo`` and deletes that queue's previous jobs first. Worker processes are real
subprocesses running ``python -m queueforge worker --app queueforge.demo:registry``.
"""

from __future__ import annotations

import itertools
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from queueforge.backoff import RetryPolicy
from queueforge.errors import PermanentError
from queueforge.migrate import migrate
from queueforge.models import Attempt, Job, JobState
from queueforge.queue import JobQueue
from queueforge.worker import JobContext, Registry

DEMO_QUEUE = "demo"
LEASE_SECONDS = 2.0
FAST_RETRY = RetryPolicy(max_attempts=3, base_seconds=0.2, cap_seconds=1.0)

registry = Registry()


@registry.task("demo.ok")
def ok(ctx: JobContext, payload: dict[str, Any]) -> None:
    ctx.sleep(float(payload.get("seconds", 0.1)))


@registry.task("demo.flaky")
def flaky(ctx: JobContext, payload: dict[str, Any]) -> None:
    ctx.sleep(0.05)
    if ctx.attempt <= int(payload["fail_times"]):
        raise RuntimeError(f"injected failure on attempt {ctx.attempt}")


@registry.task("demo.poison")
def poison(ctx: JobContext, payload: dict[str, Any]) -> None:
    raise ValueError("injected failure: this job fails on every attempt")


@registry.task("demo.invalid")
def invalid(ctx: JobContext, payload: dict[str, Any]) -> None:
    raise PermanentError("payload rejected: field 'email' is missing")


@registry.task("demo.crash_victim")
def crash_victim(ctx: JobContext, payload: dict[str, Any]) -> None:
    if ctx.attempt == 1:
        ctx.sleep(120)  # the worker running attempt 1 is killed long before this returns


@registry.task("demo.long_running")
def long_running(ctx: JobContext, payload: dict[str, Any]) -> None:
    for _ in range(600):
        ctx.sleep(0.1)  # raises JobInterrupted soon after a cancel is requested


@dataclass
class _WorkerProcess:
    worker_id: str
    proc: subprocess.Popen[bytes]
    log_path: Path


def _spawn_worker(dsn: str, worker_id: str, concurrency: int, log_dir: Path) -> _WorkerProcess:
    log_path = log_dir / f"{worker_id}.log"
    env = {**os.environ, "QUEUEFORGE_DSN": dsn}  # keep credentials out of the process list
    cmd = [
        sys.executable,
        "-m",
        "queueforge",
        "--queue",
        DEMO_QUEUE,
        "worker",
        "--app",
        "queueforge.demo:registry",
        "--worker-id",
        worker_id,
        "--concurrency",
        str(concurrency),
        "--lease-seconds",
        str(LEASE_SECONDS),
        "--poll-seconds",
        "0.2",
        "--shutdown-grace-seconds",
        "5",
    ]
    with log_path.open("wb") as log_file:
        if sys.platform == "win32":
            # A separate process group lets the demo send CTRL_BREAK_EVENT to just this worker.
            proc = subprocess.Popen(  # noqa: S603 - fixed argv, our own interpreter and module
                cmd,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = subprocess.Popen(  # noqa: S603 - fixed argv, our own interpreter and module
                cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT
            )
    return _WorkerProcess(worker_id, proc, log_path)


def _graceful_stop(worker: _WorkerProcess) -> None:
    if sys.platform == "win32":
        worker.proc.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        worker.proc.send_signal(signal.SIGTERM)


def _wait_for(predicate: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")


def _history(attempts: list[Attempt]) -> str:
    return " -> ".join(f"#{a.attempt} {a.worker_id} {a.outcome or 'running'}" for a in attempts)


def _check(results: list[tuple[bool, str]], condition: bool, description: str) -> None:
    results.append((condition, description))


def run_demo(dsn: str, *, surviving_workers: int = 2) -> int:
    if surviving_workers < 1:
        raise ValueError("surviving_workers must be >= 1")
    migrate(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM queueforge_jobs WHERE queue = %s", (DEMO_QUEUE,))
    queue = JobQueue.connect(dsn, DEMO_QUEUE)
    log_dir = Path(tempfile.mkdtemp(prefix="queueforge-demo-"))
    workers: list[_WorkerProcess] = []
    try:
        return _run(dsn, queue, log_dir, workers, surviving_workers)
    except BaseException:
        for worker in workers:
            print(f"\n--- last lines of {worker.worker_id} log ---")
            if worker.log_path.exists():
                lines = worker.log_path.read_text(errors="replace").splitlines()
                print("\n".join(lines[-20:]))
        raise
    finally:
        for worker in workers:
            if worker.proc.poll() is None:
                worker.proc.kill()
                worker.proc.wait()
        queue.close()


def _run(
    dsn: str,
    queue: JobQueue,
    log_dir: Path,
    workers: list[_WorkerProcess],
    surviving_workers: int,
) -> int:
    started = time.monotonic()

    def say(message: str) -> None:
        print(f"[{time.monotonic() - started:5.1f}s] {message}", flush=True)

    say(f"queue {DEMO_QUEUE!r}: lease {LEASE_SECONDS:.0f}s, retry policy {FAST_RETRY}")

    # 1. A worker takes a job and is killed without any chance to clean up.
    victim = queue.enqueue("demo.crash_victim", retry=FAST_RETRY).job_id
    doomed = _spawn_worker(dsn, "worker-doomed", 1, log_dir)
    workers.append(doomed)

    def victim_running() -> bool:
        job = queue.get_job(victim)
        return job.state is JobState.RUNNING and job.locked_by == "worker-doomed"

    _wait_for(victim_running, 30, "worker-doomed to claim the crash victim")
    say(f"worker-doomed claimed job {victim} (attempt 1); killing the process now")
    doomed.proc.kill()
    doomed.proc.wait()
    job = queue.get_job(victim)
    say(f"job {victim} is still '{job.state}' with a lease that nobody will renew")

    # 2. The rest of the workload.
    jobs: dict[str, list[int]] = {"ok": [], "flaky": [], "poison": [], "invalid": []}
    for i in range(20):
        jobs["ok"].append(queue.enqueue("demo.ok", {"n": i}, retry=FAST_RETRY).job_id)
    for i in range(4):
        jobs["flaky"].append(
            queue.enqueue("demo.flaky", {"n": i, "fail_times": 2}, retry=FAST_RETRY).job_id
        )
    for i in range(2):
        jobs["poison"].append(queue.enqueue("demo.poison", {"n": i}, retry=FAST_RETRY).job_id)
    jobs["invalid"].append(queue.enqueue("demo.invalid", {}, retry=FAST_RETRY).job_id)
    scheduled = queue.enqueue("demo.ok", {"scheduled": True}, delay_seconds=3).job_id
    far_future = queue.enqueue("demo.ok", {"far": True}, delay_seconds=3600).job_id
    long_job = queue.enqueue("demo.long_running", {}).job_id
    first = queue.enqueue("demo.ok", {"order": 42}, idempotency_key="order-42")
    again = queue.enqueue("demo.ok", {"order": 42}, idempotency_key="order-42")
    say(
        f"enqueued {sum(map(len, jobs.values())) + 5} more jobs; idempotency key 'order-42' "
        f"twice -> job {first.job_id} (created={first.created}), "
        f"job {again.job_id} (created={again.created})"
    )
    state = queue.cancel(far_future)
    say(f"cancelled job {far_future} (scheduled an hour ahead) while queued -> {state}")

    # 3. Surviving workers.
    for i in range(surviving_workers):
        workers.append(_spawn_worker(dsn, f"worker-{chr(ord('a') + i)}", 4, log_dir))
    say(f"started {surviving_workers} worker process(es) with concurrency 4 each")

    _wait_for(
        lambda: queue.get_job(long_job).state is JobState.RUNNING,
        30,
        "the long-running job to start",
    )
    state = queue.cancel(long_job)
    say(f"requested cancellation of running job {long_job} -> still '{state}' until it stops")

    def settled() -> bool:
        counts = queue.stats().counts
        return counts["queued"] + counts["running"] + counts["failed"] == 0

    _wait_for(settled, 90, "all jobs to reach a terminal state")
    say("every job reached a terminal state")

    # 4. Graceful shutdown of the survivors.
    survivors = workers[1:]
    for worker in survivors:
        _graceful_stop(worker)
    exit_codes = {}
    for worker in survivors:
        try:
            exit_codes[worker.worker_id] = worker.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            worker.proc.kill()
            exit_codes[worker.worker_id] = worker.proc.wait()
    say(f"graceful shutdown exit codes: {exit_codes}")

    # 5. Report.
    stats = queue.stats()
    print("\nFinal counts by state:")
    for name, count in stats.counts.items():
        print(f"  {name:<10} {count}")

    all_ids = sorted(
        {
            victim,
            scheduled,
            far_future,
            long_job,
            first.job_id,
            *itertools.chain.from_iterable(jobs.values()),
        }
    )
    print("\nAttempt histories:")
    loaded: dict[int, tuple[Job, list[Attempt]]] = {}
    for job_id in all_ids:
        job = queue.get_job(job_id)
        attempts = queue.attempts(job_id)
        loaded[job_id] = (job, attempts)
        print(f"  job {job_id:>4} {job.task:<18} {job.state:<10} {_history(attempts) or '-'}")

    print("\nLast errors of dead jobs:")
    for job_id, (job, _) in loaded.items():
        if job.state is JobState.DEAD:
            last_line = (job.last_error or "").strip().splitlines()[-1:]
            print(f"  job {job_id}: {last_line[0] if last_line else '-'}")

    # 6. Checks.
    results: list[tuple[bool, str]] = []

    def outcomes(job_id: int) -> list[str | None]:
        return [a.outcome for a in loaded[job_id][1]]

    victim_attempts = loaded[victim][1]
    _check(
        results,
        outcomes(victim) == ["lease_expired", "succeeded"]
        and victim_attempts[0].worker_id == "worker-doomed"
        and victim_attempts[1].worker_id != "worker-doomed",
        "killed worker's job: lease expired, then re-run to success by another worker",
    )
    _check(
        results,
        all(outcomes(j) == ["failed", "failed", "succeeded"] for j in jobs["flaky"]),
        "flaky jobs: two injected failures, then success on attempt 3",
    )
    _check(
        results,
        all(
            loaded[j][0].state is JobState.DEAD and outcomes(j) == ["failed"] * 3
            for j in jobs["poison"]
        ),
        "poison jobs: dead-lettered after max_attempts=3",
    )
    _check(
        results,
        all(
            loaded[j][0].state is JobState.DEAD and outcomes(j) == ["failed"]
            for j in jobs["invalid"]
        ),
        "PermanentError: dead-lettered after one attempt",
    )
    _check(
        results,
        loaded[far_future][0].state is JobState.CANCELLED and outcomes(far_future) == [],
        "queued job cancelled before it ever ran",
    )
    _check(
        results,
        loaded[long_job][0].state is JobState.CANCELLED and outcomes(long_job) == ["cancelled"],
        "running job cancelled cooperatively",
    )
    _check(
        results,
        first.job_id == again.job_id and first.created and not again.created,
        "idempotent enqueue returned the existing job",
    )
    _check(
        results,
        all(
            outcomes(j).count("succeeded") == 1
            for j, (job, _) in loaded.items()
            if job.state is JobState.SUCCEEDED
        ),
        "every succeeded job has exactly one successful attempt",
    )
    _check(
        results,
        all(
            later.started_at >= earlier.finished_at
            for _, attempts in loaded.values()
            for earlier, later in itertools.pairwise(attempts)
            if earlier.finished_at is not None
        ),
        "no job's attempts overlapped in time",
    )
    _check(
        results,
        all(code == 0 for code in exit_codes.values()),
        "surviving workers shut down gracefully (exit code 0)",
    )
    print("\nChecks:")
    for passed, description in results:
        print(f"  [{'PASS' if passed else 'FAIL'}] {description}")
    failed = sum(1 for passed, _ in results if not passed)
    print(f"\n{len(results) - failed}/{len(results)} checks passed")
    return 0 if failed == 0 else 1
