"""Handlers used by tests that run workers in subprocesses (`--app qf_test_app:registry`)."""

from __future__ import annotations

import os
import random
import time
from typing import Any

import psycopg

from queueforge import JobContext, Registry

registry = Registry()
PROBE_LOCK_CLASS = 7731


@registry.task("block_first_attempt")
def block_first_attempt(ctx: JobContext, payload: dict[str, Any]) -> None:
    # Deliberately ignores ctx: models a handler stuck in a call when its process dies.
    if ctx.attempt == 1:
        time.sleep(60)


@registry.task("cooperative_wait")
def cooperative_wait(ctx: JobContext, payload: dict[str, Any]) -> None:
    for _ in range(600):
        ctx.sleep(0.1)


@registry.task("probe")
def probe(ctx: JobContext, payload: dict[str, Any]) -> None:
    """Detects concurrent execution across processes with a PostgreSQL advisory lock."""
    with psycopg.connect(os.environ["QUEUEFORGE_DSN"], autocommit=True) as conn:
        _probe(conn, ctx, payload)


def _probe(conn: psycopg.Connection[Any], ctx: JobContext, payload: dict[str, Any]) -> None:
    worker = ctx.job.locked_by
    conn.execute(
        "INSERT INTO qf_test_executions (job_id, attempt, worker) VALUES (%s, %s, %s)",
        (ctx.job_id, ctx.attempt, worker),
    )
    row = conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s)", (PROBE_LOCK_CLASS, ctx.job_id)
    ).fetchone()
    if row is None or not row[0]:
        conn.execute(
            "INSERT INTO qf_test_violations (job_id, attempt, worker) VALUES (%s, %s, %s)",
            (ctx.job_id, ctx.attempt, worker),
        )
        return
    try:
        time.sleep(random.uniform(0, 0.01))
        if random.random() < float(payload.get("fail_rate", 0)):
            raise RuntimeError("injected probe failure")
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s, %s)", (PROBE_LOCK_CLASS, ctx.job_id))
