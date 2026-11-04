"""The schema enforces the state machine; the README diagram must match it."""

import re
from pathlib import Path

import psycopg
import pytest
from psycopg import errors

from queueforge import JobQueue, migrate

README = Path(__file__).resolve().parent.parent / "README.md"


def _transitions(dsn: str) -> set[tuple[str, str]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT from_state, to_state FROM queueforge_transitions").fetchall()
    return {(a, b) for a, b in rows}


def test_migrate_is_idempotent(dsn: str) -> None:
    assert migrate(dsn) == []


def test_readme_state_diagram_matches_sql_transitions(dsn: str) -> None:
    text = README.read_text(encoding="utf-8")
    block = re.search(r"```mermaid\n(stateDiagram-v2\n.*?)```", text, re.S)
    assert block, "README must contain a Mermaid stateDiagram-v2 block"
    edges = set(re.findall(r"^\s*(\S+)\s*-->\s*(\w+)", block.group(1), re.M))
    diagram = {(a, b) for a, b in edges if a != "[*]"}
    assert diagram == _transitions(dsn)


@pytest.mark.parametrize("target", ["succeeded", "dead", "failed"])
def test_trigger_rejects_illegal_transitions(dsn: str, queue: JobQueue, target: str) -> None:
    job_id = queue.enqueue("t").job_id
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="illegal"):
        conn.execute(
            "UPDATE queueforge_jobs SET state = %s, finished_at = now() WHERE id = %s",
            (target, job_id),
        )


def test_succeeded_is_terminal(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    job = queue.claim("w")[0]
    assert job.lease_token is not None
    queue.complete(job_id, job.lease_token)
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="illegal"):
        conn.execute(
            "UPDATE queueforge_jobs SET state = 'queued', finished_at = NULL WHERE id = %s",
            (job_id,),
        )


def test_jobs_must_be_inserted_as_queued(dsn: str, queue_name: str) -> None:
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="inserted"):
        conn.execute(
            "INSERT INTO queueforge_jobs (queue, task, state) VALUES (%s, 't', 'succeeded')",
            (queue_name,),
        )


def test_running_requires_a_lease(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="lease"):
        conn.execute("UPDATE queueforge_jobs SET state = 'running' WHERE id = %s", (job_id,))


def test_attempts_never_decrease(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    queue.claim("w")
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="decrease"):
        conn.execute("UPDATE queueforge_jobs SET attempts = 0 WHERE id = %s", (job_id,))


def test_running_job_lease_token_cannot_be_swapped(dsn: str, queue: JobQueue) -> None:
    job_id = queue.enqueue("t").job_id
    queue.claim("w")
    with psycopg.connect(dsn) as conn, pytest.raises(errors.CheckViolation, match="token"):
        conn.execute(
            "UPDATE queueforge_jobs SET lease_token = gen_random_uuid() WHERE id = %s", (job_id,)
        )
