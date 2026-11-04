import random

import pytest

from queueforge import RetryPolicy, backoff_ceiling, full_jitter_delay


def test_ceiling_doubles_from_base_until_cap() -> None:
    assert [backoff_ceiling(n, 1.0, 10.0) for n in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_ceiling_with_zero_base_is_zero() -> None:
    assert backoff_ceiling(5, 0.0, 0.0) == 0.0


def test_ceiling_does_not_overflow_for_huge_attempt_counts() -> None:
    assert backoff_ceiling(100_000, 1.0, 300.0) == 300.0


def test_ceiling_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError, match="attempt"):
        backoff_ceiling(0, 1.0, 10.0)


def test_full_jitter_scales_the_ceiling_by_the_random_draw() -> None:
    assert full_jitter_delay(3, 1.0, 10.0, rng=lambda: 0.5) == 2.0
    assert full_jitter_delay(3, 1.0, 10.0, rng=lambda: 0.0) == 0.0


def test_full_jitter_spreads_over_the_whole_window() -> None:
    rng = random.Random(1234)
    samples = [full_jitter_delay(4, 1.0, 100.0, rng=rng.random) for _ in range(2000)]
    assert all(0.0 <= s < 8.0 for s in samples)
    assert min(samples) < 0.5
    assert max(samples) > 7.5


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"base_seconds": -1.0}, "base_seconds"),
        ({"base_seconds": 10.0, "cap_seconds": 5.0}, "cap_seconds"),
    ],
)
def test_retry_policy_rejects_invalid_values(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
