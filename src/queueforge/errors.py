"""Exceptions raised by queueforge."""

from __future__ import annotations


class QueueForgeError(Exception):
    """Base class for queueforge errors."""


class JobNotFound(QueueForgeError):
    """No job with the given id exists."""


class LeaseLost(QueueForgeError):
    """The caller's lease is no longer valid, so its state change was rejected.

    This is the fencing check: the lease token did not match, the job is no longer running, or
    the lease expired. The caller's result must be discarded; another attempt owns the job now or
    will own it after the reaper runs.
    """


class IdempotencyConflict(QueueForgeError):
    """An enqueue reused an idempotency key with a different task or payload."""


class InvalidTransition(QueueForgeError):
    """The requested operation does not apply to the job's current state."""


class PermanentError(Exception):
    """Raise from a handler to dead-letter the job immediately, without further retries."""


class JobInterrupted(Exception):
    """Raised by JobContext.check() when the worker asked the handler to stop."""
