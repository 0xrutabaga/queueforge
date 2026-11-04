import json

import pytest

from queueforge.cli import EXIT_DB_UNAVAILABLE, describe_dsn, load_registry, main


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str, str]:
    code = main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def test_unreachable_database_exits_with_a_clear_message_and_no_password(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = "postgresql://someone:hunter2-secret@127.0.0.1:1/nothing"
    code, _, err = run(capsys, "--dsn", dsn, "stats")
    assert code == EXIT_DB_UNAVAILABLE
    assert "cannot connect to PostgreSQL at 127.0.0.1:1/nothing" in err
    assert "hunter2" not in err


def test_describe_dsn_hides_credentials() -> None:
    assert describe_dsn("postgresql://u:p@db.internal:6543/app") == "db.internal:6543/app"
    assert describe_dsn("not a dsn ===") == "<unparseable DSN>"


def test_enqueue_show_cancel_and_stats(
    dsn: str, queue_name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    base = ["--dsn", dsn, "--queue", queue_name]
    code, out, _ = run(capsys, *base, "enqueue", "report", "--payload", '{"day": 1}', "--key", "d1")
    assert code == 0
    job_id = json.loads(out)["job_id"]
    code, out, _ = run(capsys, *base, "enqueue", "report", "--payload", '{"day": 1}', "--key", "d1")
    assert json.loads(out) == {"job_id": job_id, "created": False}

    code, out, _ = run(capsys, *base, "stats", "--json")
    assert json.loads(out)["counts"]["queued"] == 1

    code, out, _ = run(capsys, *base, "cancel", str(job_id))
    assert json.loads(out) == {"job_id": job_id, "state": "cancelled"}

    code, out, _ = run(capsys, *base, "show", str(job_id))
    shown = json.loads(out)
    assert shown["state"] == "cancelled" and shown["history"] == []

    code, out, _ = run(capsys, *base, "stats")
    assert "cancelled  1" in out

    code, out, _ = run(capsys, *base, "reap")
    assert json.loads(out) == []


def test_cli_reports_operator_errors(
    dsn: str, queue_name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    base = ["--dsn", dsn, "--queue", queue_name]
    code, _, err = run(capsys, *base, "enqueue", "t", "--payload", "[1, 2]")
    assert code == 2 and "JSON object" in err
    code, _, err = run(capsys, *base, "enqueue", "t", "--payload", "{not json")
    assert code == 1 and "error" in err
    code, _, err = run(capsys, *base, "show", "-1")
    assert code == 1 and "not found" in err
    code, out, _ = run(capsys, *base, "enqueue", "t")
    code, _, err = run(capsys, *base, "requeue", str(json.loads(out)["job_id"]))
    assert code == 1 and "only dead jobs" in err


def test_migrate_command(dsn: str, capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = run(capsys, "--dsn", dsn, "migrate")
    assert code == 0 and "up to date" in out


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("no_colon", "must look like"),
        ("queueforge.demo:missing", "not a queueforge.worker.Registry"),
        ("queueforge.demo:run_demo", "not a queueforge.worker.Registry"),
    ],
)
def test_load_registry_rejects_bad_specs(spec: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        load_registry(spec)


def test_load_registry_accepts_the_demo_app() -> None:
    assert "demo.ok" in load_registry("queueforge.demo:registry").names()
