"""Apply the plain SQL migrations shipped in queueforge/migrations, in filename order."""

from __future__ import annotations

from importlib import resources

import psycopg

# Arbitrary constant key for pg_advisory_xact_lock so concurrent `migrate` calls serialise.
_MIGRATION_LOCK_KEY = 7_316_027_001


def migration_files() -> list[tuple[str, str]]:
    """Return (version, sql) pairs sorted by filename."""
    package = resources.files("queueforge.migrations")
    files = sorted(
        (entry.name, entry.read_text(encoding="utf-8"))
        for entry in package.iterdir()
        if entry.name.endswith(".sql")
    )
    return [(name.removesuffix(".sql"), sql) for name, sql in files]


def migrate(conninfo: str) -> list[str]:
    """Apply pending migrations. Returns the versions applied by this call."""
    applied: list[str] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS queueforge_schema_migrations ("
            " version text PRIMARY KEY,"
            " applied_at timestamptz NOT NULL DEFAULT now())"
        )
        for version, sql in migration_files():
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK_KEY,))
                done = conn.execute(
                    "SELECT 1 FROM queueforge_schema_migrations WHERE version = %s", (version,)
                ).fetchone()
                if done:
                    continue
                # The migration files are trusted package data, not user input.
                conn.execute(sql.encode("utf-8"))
                conn.execute(
                    "INSERT INTO queueforge_schema_migrations (version) VALUES (%s)", (version,)
                )
                applied.append(version)
    return applied
