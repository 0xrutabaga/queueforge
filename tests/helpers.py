"""Shared test helpers."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg

from queueforge import Worker

TESTS_DIR = Path(__file__).resolve().parent


def wait_for(predicate: Callable[[], bool], timeout: float = 15.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


@contextmanager
def running(worker: Worker) -> Iterator[Worker]:
    """Run a worker on a background thread for the duration of the block."""
    result: dict[str, Any] = {}

    def target() -> None:
        result["report"] = worker.run()

    thread = threading.Thread(target=target, name=f"test-{worker.worker_id}", daemon=True)
    thread.start()
    try:
        yield worker
    finally:
        worker.stop()
        thread.join(timeout=30)
        assert not thread.is_alive(), "worker did not stop"


def sql(dsn: str, query: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.execute(query, params)  # type: ignore[arg-type]
        return cur.fetchall() if cur.description else []


def expire_lease(dsn: str, job_id: int) -> None:
    """Make a running job's lease expire now, without waiting for it."""
    sql(
        dsn,
        "UPDATE queueforge_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (job_id,),
    )


def make_due(dsn: str, job_id: int) -> None:
    """Skip a scheduled job's remaining delay."""
    sql(dsn, "UPDATE queueforge_jobs SET run_at = now() WHERE id = %s", (job_id,))


def spawn_worker(
    dsn: str, queue: str, worker_id: str, log_path: Path, *extra: str
) -> subprocess.Popen[bytes]:
    """Start `python -m queueforge worker` with the test handler app in a real subprocess."""
    env = {
        **os.environ,
        "QUEUEFORGE_DSN": dsn,
        "PYTHONPATH": os.pathsep.join([str(TESTS_DIR), os.environ.get("PYTHONPATH", "")]),
    }
    cmd = [
        sys.executable,
        "-m",
        "queueforge",
        "--queue",
        queue,
        "worker",
        "--app",
        "qf_test_app:registry",
        "--worker-id",
        worker_id,
        "--poll-seconds",
        "0.05",
        *extra,
    ]
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
    with log_path.open("wb") as log_file:
        return subprocess.Popen(
            cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT, creationflags=flags
        )
