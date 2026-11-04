from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow
from psycopg_pool import ConnectionPool

from queueforge import JobQueue, migrate
from queueforge.cli import DEFAULT_DSN, check_database

TEST_DSN = os.environ.get("QUEUEFORGE_TEST_DSN") or os.environ.get("QUEUEFORGE_DSN") or DEFAULT_DSN


@pytest.fixture(scope="session")
def dsn() -> str:
    error = check_database(TEST_DSN)
    if error is not None:
        pytest.fail(f"{error}. Start it with `docker compose up -d --wait`.", pytrace=False)
    migrate(TEST_DSN)
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS qf_test_executions "
            "(job_id bigint, attempt int, worker text, at timestamptz DEFAULT clock_timestamp())"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS qf_test_violations"
            " (job_id bigint, attempt int, worker text)"
        )
    return TEST_DSN


@pytest.fixture(scope="session")
def pool(dsn: str) -> Iterator[ConnectionPool[psycopg.Connection[TupleRow]]]:
    p: ConnectionPool[psycopg.Connection[TupleRow]] = ConnectionPool(
        dsn, min_size=2, max_size=40, open=True, timeout=30
    )
    yield p
    p.close()


@pytest.fixture
def queue_name(request: pytest.FixtureRequest) -> str:
    # A fresh queue per test isolates tests without truncating tables.
    return f"t-{request.node.name[:40]}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def queue(pool: ConnectionPool[Any], queue_name: str) -> JobQueue:
    # rng=1.0 makes every backoff delay equal to its ceiling, so schedules are exact.
    return JobQueue(pool, queue_name, rng=lambda: 1.0)


@pytest.fixture
def jittered_queue(pool: ConnectionPool[Any], queue_name: str) -> JobQueue:
    return JobQueue(pool, queue_name)
