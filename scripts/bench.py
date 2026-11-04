"""Rough throughput measurement for no-op jobs. Not a benchmark of anything but this setup.

    uv run python scripts/bench.py --jobs 2000 --workers 4 --concurrency 8

Runs workers in this process (threads), against the database in $QUEUEFORGE_DSN or the docker
compose default. Each job costs at least two transactions (claim, complete), so results mostly
measure round trips to PostgreSQL. Print the machine you ran it on next to any number you quote.
"""

from __future__ import annotations

import argparse
import os
import platform
import threading
import time
import uuid

import psycopg

from queueforge import JobContext, JobQueue, Registry, Worker, migrate
from queueforge.cli import DEFAULT_DSN


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()
    dsn = os.environ.get("QUEUEFORGE_DSN", DEFAULT_DSN)
    migrate(dsn)

    name = f"bench-{uuid.uuid4().hex[:8]}"
    queue = JobQueue.connect(dsn, name, max_connections=args.workers * (args.concurrency + 2))
    registry = Registry()

    @registry.task("noop")
    def noop(ctx: JobContext, payload: object) -> None:
        return None

    try:
        start = time.perf_counter()
        for i in range(args.jobs):
            queue.enqueue("noop", {"i": i})
        enqueue_seconds = time.perf_counter() - start

        workers = [
            Worker(queue, registry, concurrency=args.concurrency, poll_seconds=0.01)
            for _ in range(args.workers)
        ]
        threads = [threading.Thread(target=w.run) for w in workers]
        start = time.perf_counter()
        for t in threads:
            t.start()
        while queue.stats().counts["succeeded"] < args.jobs:
            time.sleep(0.05)
        process_seconds = time.perf_counter() - start
        for w in workers:
            w.stop()
        for t in threads:
            t.join()
    finally:
        queue.close()
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("DELETE FROM queueforge_jobs WHERE queue = %s", (name,))

    print(
        f"machine: {platform.system()} {platform.release()}, {platform.machine()}, "
        f"{os.cpu_count()} logical CPUs, Python {platform.python_version()}"
    )
    print(f"jobs={args.jobs} workers={args.workers} concurrency={args.concurrency}")
    print(
        f"enqueue (one transaction per job, one client thread): "
        f"{args.jobs / enqueue_seconds:.0f} jobs/s"
    )
    print(f"claim+run+complete (no-op handler): {args.jobs / process_seconds:.0f} jobs/s")


if __name__ == "__main__":
    main()
