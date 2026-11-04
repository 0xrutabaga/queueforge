"""Retry delay computation: capped exponential backoff with full jitter."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

# Non-cryptographic randomness is correct here: jitter only spreads retries over time.
_default_rng = random.Random()  # noqa: S311


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How often and how fast a job is retried. Stored on each job row at enqueue time."""

    max_attempts: int = 5
    base_seconds: float = 1.0
    cap_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_seconds < 0:
            raise ValueError("base_seconds must be >= 0")
        if self.cap_seconds < self.base_seconds:
            raise ValueError("cap_seconds must be >= base_seconds")


def backoff_ceiling(attempt: int, base_seconds: float, cap_seconds: float) -> float:
    """Upper bound of the delay after failed attempt n (1-based): min(cap, base * 2^(n-1))."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    # Clamp the exponent so huge attempt counts cannot overflow a float.
    exponent = min(attempt - 1, 1023)
    return min(cap_seconds, base_seconds * (2.0**exponent))


def full_jitter_delay(
    attempt: int,
    base_seconds: float,
    cap_seconds: float,
    rng: Callable[[], float] = _default_rng.random,
) -> float:
    """Delay before retrying after failed attempt `attempt`: uniform in [0, ceiling).

    This is the "full jitter" strategy: spreading retries uniformly over the whole window
    avoids synchronised retry storms when many jobs fail at the same moment.
    """
    return rng() * backoff_ceiling(attempt, base_seconds, cap_seconds)
