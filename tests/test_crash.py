"""Real worker processes: hard kill mid-job, and graceful shutdown by signal."""

import signal
import subprocess
import sys
from pathlib import Path

from helpers import running, spawn_worker, wait_for
from qf_test_app import registry

from queueforge import JobQueue, JobState, RetryPolicy, Worker

LEASE = "1.5"


def test_killed_worker_job_is_rerun_after_lease_expiry(
    dsn: str, queue: JobQueue, tmp_path: Path
) -> None:
    job_id = queue.enqueue(
        "block_first_attempt", retry=RetryPolicy(max_attempts=3, base_seconds=0, cap_seconds=0)
    ).job_id
    proc = spawn_worker(
        dsn, queue.queue, "doomed", tmp_path / "doomed.log", "--lease-seconds", LEASE
    )
    try:
        wait_for(lambda: queue.get_job(job_id).state is JobState.RUNNING, what="claim")
        # Hard kill: SIGKILL on POSIX, TerminateProcess on Windows. No cleanup code runs.
        proc.kill()
        proc.wait(10)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert queue.get_job(job_id).state is JobState.RUNNING  # nobody told the database

    rescuer = Worker(queue, registry, worker_id="rescuer", lease_seconds=1.5, poll_seconds=0.05)
    with running(rescuer):
        wait_for(lambda: queue.get_job(job_id).state is JobState.SUCCEEDED, timeout=20)

    history = [(a.worker_id, a.outcome) for a in queue.attempts(job_id)]
    assert history == [("doomed", "lease_expired"), ("rescuer", "succeeded")]
    assert queue.get_job(job_id).attempts == 2


def _graceful_signal(proc: subprocess.Popen[bytes]) -> None:
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)  # worker runs in its own process group
    else:
        proc.send_signal(signal.SIGTERM)


def test_signal_triggers_graceful_shutdown_and_release(
    dsn: str, queue: JobQueue, tmp_path: Path
) -> None:
    job_id = queue.enqueue("cooperative_wait").job_id
    log = tmp_path / "graceful.log"
    proc = spawn_worker(dsn, queue.queue, "graceful", log, "--lease-seconds", "10")
    try:
        wait_for(lambda: queue.get_job(job_id).state is JobState.RUNNING, what="claim")
        _graceful_signal(proc)
        code = proc.wait(20)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert code == 0, log.read_text(errors="replace")
    assert queue.get_job(job_id).state is JobState.QUEUED
    assert [a.outcome for a in queue.attempts(job_id)] == ["released"]
    assert "shutting down" in log.read_text(errors="replace")
