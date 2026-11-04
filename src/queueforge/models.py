"""Plain data types returned by the queue API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # an attempt failed; a retry is scheduled at run_at
    DEAD = "dead"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED})


@dataclass(frozen=True, slots=True)
class Job:
    id: int
    queue: str
    task: str
    payload: Any
    state: JobState
    priority: int
    run_at: datetime
    attempts: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    idempotency_key: str | None
    lease_token: UUID | None
    lease_expires_at: datetime | None
    locked_by: str | None
    heartbeat_at: datetime | None
    cancel_requested: bool
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Job:
        # Ignore unknown columns so a later additive migration does not break older clients.
        values = {name: row[name] for name in cls.__dataclass_fields__}
        values["state"] = JobState(values["state"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class Attempt:
    job_id: int
    attempt: int
    worker_id: str
    lease_token: UUID
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    job_id: int
    created: bool  # False when an existing job with the same idempotency key was returned


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    job_id: int
    lease_token: UUID  # identifies the attempt: one job can have a stale and a live attempt
    cancel_requested: bool


@dataclass(frozen=True, slots=True)
class QueueStats:
    queue: str
    counts: dict[str, int]
    oldest_runnable_age_seconds: float | None
    expired_leases: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "queue": self.queue,
            "counts": self.counts,
            "oldest_runnable_age_seconds": self.oldest_runnable_age_seconds,
            "expired_leases": self.expired_leases,
        }
