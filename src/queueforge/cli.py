"""Command line interface: ``queueforge <command>``."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sys
from collections.abc import Sequence
from types import FrameType
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from queueforge.backoff import RetryPolicy
from queueforge.demo import run_demo
from queueforge.errors import QueueForgeError
from queueforge.migrate import migrate
from queueforge.queue import JobQueue
from queueforge.worker import Registry, Worker

# Matches docker-compose.yml. Local development credentials only.
DEFAULT_DSN = "postgresql://queueforge:queueforge@127.0.0.1:55432/queueforge"

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DB_UNAVAILABLE = 3
EXIT_ERROR = 1


def describe_dsn(dsn: str) -> str:
    """host:port/dbname without credentials, for error messages."""
    try:
        parts = conninfo_to_dict(dsn)
    except psycopg.ProgrammingError:
        return "<unparseable DSN>"
    return f"{parts.get('host', 'localhost')}:{parts.get('port', 5432)}/{parts.get('dbname', '')}"


def check_database(dsn: str, timeout_seconds: int = 5) -> str | None:
    """Return an error message if PostgreSQL is unreachable, else None."""
    try:
        with psycopg.connect(dsn, connect_timeout=timeout_seconds):
            return None
    except psycopg.Error as exc:
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        return f"cannot connect to PostgreSQL at {describe_dsn(dsn)}: {first_line}"


def load_registry(spec: str) -> Registry:
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"--app must look like 'package.module:registry', got {spec!r}")
    module = importlib.import_module(module_name)
    registry = getattr(module, attr, None)
    if not isinstance(registry, Registry):
        raise ValueError(f"{spec!r} is not a queueforge.worker.Registry")
    return registry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queueforge", description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("QUEUEFORGE_DSN", DEFAULT_DSN),
        help="PostgreSQL connection string (default: $QUEUEFORGE_DSN or the docker compose DB)",
    )
    parser.add_argument("--queue", default="default", help="queue name (default: default)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply SQL migrations")

    w = sub.add_parser("worker", help="run a worker until SIGINT/SIGTERM")
    w.add_argument("--app", required=True, help="handler registry, e.g. myapp.jobs:registry")
    w.add_argument("--concurrency", type=int, default=4)
    w.add_argument("--lease-seconds", type=float, default=30.0)
    w.add_argument("--heartbeat-seconds", type=float, default=None)
    w.add_argument("--poll-seconds", type=float, default=1.0)
    w.add_argument("--job-timeout-seconds", type=float, default=None)
    w.add_argument("--shutdown-grace-seconds", type=float, default=10.0)
    w.add_argument("--worker-id", default=None)

    e = sub.add_parser("enqueue", help="enqueue one job")
    e.add_argument("task")
    e.add_argument("--payload", default="{}", help="JSON object")
    e.add_argument("--key", default=None, help="idempotency key")
    e.add_argument("--delay-seconds", type=float, default=0.0)
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--max-attempts", type=int, default=5)
    e.add_argument("--backoff-base-seconds", type=float, default=1.0)
    e.add_argument("--backoff-cap-seconds", type=float, default=300.0)

    s = sub.add_parser("stats", help="counts by state, oldest runnable age, expired leases")
    s.add_argument("--json", action="store_true")

    show = sub.add_parser("show", help="show a job and its attempt history")
    show.add_argument("job_id", type=int)

    c = sub.add_parser("cancel", help="cancel a job (cooperative if running)")
    c.add_argument("job_id", type=int)

    r = sub.add_parser("requeue", help="redrive a dead job")
    r.add_argument("job_id", type=int)
    r.add_argument("--extra-attempts", type=int, default=1)

    sub.add_parser("reap", help="settle expired leases once")

    d = sub.add_parser("demo", help="run the crash/retry/dead-letter demo against the DB")
    d.add_argument("--workers", type=int, default=2, help="surviving worker processes")

    return parser


def _run_worker(args: argparse.Namespace, queue: JobQueue) -> int:
    registry = load_registry(args.app)
    worker = Worker(
        queue,
        registry,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        lease_seconds=args.lease_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
        poll_seconds=args.poll_seconds,
        job_timeout_seconds=args.job_timeout_seconds,
        shutdown_grace_seconds=args.shutdown_grace_seconds,
    )

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        logging.getLogger("queueforge.cli").info("received signal %d, shutting down", signum)
        worker.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if sys.platform == "win32":  # CTRL_BREAK_EVENT sent by a parent process, as the demo does
        signal.signal(signal.SIGBREAK, on_signal)
    report = worker.run()
    return EXIT_OK if not report.abandoned_jobs else EXIT_ERROR


def _show(queue: JobQueue, job_id: int) -> dict[str, Any]:
    job = queue.get_job(job_id)
    return {
        "id": job.id,
        "queue": job.queue,
        "task": job.task,
        "state": job.state.value,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "run_at": job.run_at.isoformat(),
        "cancel_requested": job.cancel_requested,
        "last_error": job.last_error,
        "history": [
            {
                "attempt": a.attempt,
                "worker": a.worker_id,
                "outcome": a.outcome,
                "started_at": a.started_at.isoformat(),
                "finished_at": a.finished_at.isoformat() if a.finished_at else None,
                "error": a.error,
            }
            for a in queue.attempts(job_id)
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    )
    error = check_database(args.dsn)
    if error is not None:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_DB_UNAVAILABLE

    if args.command == "migrate":
        applied = migrate(args.dsn)
        print(f"applied: {', '.join(applied)}" if applied else "schema is up to date")
        return EXIT_OK

    if args.command == "demo":
        return run_demo(args.dsn, surviving_workers=args.workers)

    max_connections = args.concurrency + 2 if args.command == "worker" else 2
    queue = JobQueue.connect(args.dsn, args.queue, max_connections=max_connections)
    try:
        if args.command == "worker":
            return _run_worker(args, queue)
        if args.command == "enqueue":
            payload = json.loads(args.payload)
            if not isinstance(payload, dict):
                print("error: --payload must be a JSON object", file=sys.stderr)
                return EXIT_USAGE
            result = queue.enqueue(
                args.task,
                payload,
                priority=args.priority,
                delay_seconds=args.delay_seconds,
                idempotency_key=args.key,
                retry=RetryPolicy(
                    args.max_attempts, args.backoff_base_seconds, args.backoff_cap_seconds
                ),
            )
            print(json.dumps({"job_id": result.job_id, "created": result.created}))
        elif args.command == "stats":
            stats = queue.stats()
            if args.json:
                print(json.dumps(stats.as_dict()))
            else:
                for state, count in stats.counts.items():
                    print(f"{state:<10} {count}")
                age = stats.oldest_runnable_age_seconds
                print(f"oldest runnable age: {'-' if age is None else f'{age:.1f}s'}")
                print(f"expired leases: {stats.expired_leases}")
        elif args.command == "show":
            print(json.dumps(_show(queue, args.job_id), indent=2))
        elif args.command == "cancel":
            print(json.dumps({"job_id": args.job_id, "state": queue.cancel(args.job_id)}))
        elif args.command == "requeue":
            queue.requeue_dead(args.job_id, extra_attempts=args.extra_attempts)
            print(json.dumps({"job_id": args.job_id, "state": "queued"}))
        elif args.command == "reap":
            settled = queue.reap_expired()
            print(json.dumps([{"job_id": job_id, "state": state} for job_id, state in settled]))
    except (QueueForgeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        queue.close()
    return EXIT_OK
