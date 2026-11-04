"""Pure policy units: classification, breaker, token bucket, AIMD, retry budget, deadline."""

from __future__ import annotations

import pytest

from gateway.providers.base import parse_retry_after
from gateway.resilience.breaker import BreakerPolicy, BreakerState, LocalBreaker
from gateway.resilience.outcomes import BREAKER_FAILURES, Outcome, classify_status, is_retryable
from gateway.resilience.ratelimit import AimdState, LocalTokenBucket
from gateway.resilience.retry import RetryBudget
from gateway.routing.deadline import Deadline

# ---------------------------------------------------------------- classification


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, Outcome.SUCCESS), (201, Outcome.SUCCESS),
        (400, Outcome.CLIENT_ERROR), (404, Outcome.CLIENT_ERROR), (422, Outcome.CLIENT_ERROR),
        (408, Outcome.REJECTED), (429, Outcome.RATE_LIMITED),
        (502, Outcome.REJECTED), (503, Outcome.REJECTED),
        (500, Outcome.SERVER_ERROR), (504, Outcome.SERVER_ERROR), (599, Outcome.SERVER_ERROR),
    ],
)
def test_classify_status(status: int, expected: Outcome) -> None:
    assert classify_status(status) is expected


def test_connect_timeout_safe_read_timeout_not() -> None:
    # "Connect-timeout is safe to retry, read-timeout is not."
    assert is_retryable(Outcome.CONNECT_ERROR, replay_safe=False)
    assert not is_retryable(Outcome.TIMEOUT, replay_safe=False)
    assert is_retryable(Outcome.TIMEOUT, replay_safe=True)


def test_retry_matrix() -> None:
    for outcome in (Outcome.RATE_LIMITED, Outcome.REJECTED, Outcome.CONNECT_ERROR):
        assert is_retryable(outcome, replay_safe=False)
    for outcome in (Outcome.SERVER_ERROR, Outcome.PROTOCOL_ERROR, Outcome.TIMEOUT):
        assert not is_retryable(outcome, replay_safe=False)
    for outcome in (Outcome.SUCCESS, Outcome.CLIENT_ERROR):
        assert not is_retryable(outcome, replay_safe=True)


def test_429_and_client_errors_do_not_trip_breaker() -> None:
    assert Outcome.RATE_LIMITED not in BREAKER_FAILURES
    assert Outcome.CLIENT_ERROR not in BREAKER_FAILURES
    assert Outcome.TIMEOUT in BREAKER_FAILURES


def test_parse_retry_after() -> None:
    assert parse_retry_after("2") == 2000
    assert parse_retry_after("0.5") == 500
    assert parse_retry_after(None) == 0
    assert parse_retry_after("garbage") == 0
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:10 GMT", now=1445412480.0) == 10_000


# ---------------------------------------------------------------- breaker


def _breaker() -> LocalBreaker:
    return LocalBreaker(BreakerPolicy(window_s=10, failure_ratio=0.5, min_volume=20, cooldown_ms=5000))


def test_breaker_min_volume_gate() -> None:
    b = _breaker()
    for _ in range(19):
        b.record(1_000, failed=True)
    assert b.tick(1_500) is BreakerState.CLOSED  # 19 < min_volume


def test_breaker_trips_on_ratio_and_recovers_through_single_probe() -> None:
    b = _breaker()
    for i in range(20):
        b.record(1_000, failed=i < 11)  # 11/20 = 55% > 50%
    assert b.tick(1_500) is BreakerState.OPEN
    assert not b.try_acquire_probe()
    assert b.tick(6_499) is BreakerState.OPEN            # cooldown not over
    assert b.tick(6_500) is BreakerState.HALF_OPEN
    assert b.try_acquire_probe()
    assert not b.try_acquire_probe()                     # exactly one probe in flight
    b.probe_result(7_000, success=True)
    assert b.state is BreakerState.CLOSED
    assert b.tick(7_100) is BreakerState.CLOSED          # window was reset on close


def test_breaker_half_open_failure_reopens() -> None:
    b = _breaker()
    for _ in range(20):
        b.record(1_000, failed=True)
    b.tick(1_000)
    b.tick(6_000)
    assert b.try_acquire_probe()
    b.probe_result(6_100, success=False)
    assert b.state is BreakerState.OPEN
    assert b.tick(11_000) is BreakerState.OPEN
    assert b.tick(11_100) is BreakerState.HALF_OPEN


def test_breaker_exact_ratio_does_not_trip() -> None:
    b = _breaker()
    for i in range(20):
        b.record(1_000, failed=i < 10)  # exactly 50%: threshold is strictly greater
    assert b.tick(1_000) is BreakerState.CLOSED


def test_breaker_old_buckets_expire() -> None:
    b = _breaker()
    for _ in range(30):
        b.record(1_000, failed=True)
    assert b.tick(11_000) is BreakerState.CLOSED  # second 1 is outside (11-10, 11]


# ---------------------------------------------------------------- token bucket / AIMD


def test_token_bucket_burst_then_refill() -> None:
    bucket = LocalTokenBucket(rate=10, burst=5, now_ms=0)
    assert all(bucket.acquire(0)[0] for _ in range(5))
    allowed, retry_after = bucket.acquire(0)
    assert not allowed and retry_after == 100            # 1 token at 10/s
    assert bucket.acquire(100)[0]
    assert not bucket.acquire(100)[0]


def test_token_bucket_never_exceeds_burst() -> None:
    bucket = LocalTokenBucket(rate=100, burst=3, now_ms=0)
    granted = sum(bucket.acquire(60_000)[0] for _ in range(10))
    assert granted == 3


def test_aimd_halves_once_per_window_and_recovers() -> None:
    aimd = AimdState(max_rps=200, min_rps=10, factor=0.5, step=10, window_ms=1000)
    aimd.on_rate_limited(0)
    aimd.on_rate_limited(10)        # same window: one signal, not two
    aimd.on_rate_limited(999)
    assert aimd.effective_rps == 100
    aimd.on_rate_limited(1_000)
    assert aimd.effective_rps == 50
    aimd.tick(1_500)                # not a clean window yet
    assert aimd.effective_rps == 50
    aimd.tick(2_000)
    assert aimd.effective_rps == 60
    aimd.tick(2_500)                # one increase per window
    assert aimd.effective_rps == 60
    for t in range(3_000, 30_000, 1_000):
        aimd.tick(t)
    assert aimd.effective_rps == 200  # capped at configured


def test_aimd_floor() -> None:
    aimd = AimdState(max_rps=100, min_rps=30, factor=0.5, step=5, window_ms=1000)
    for t in range(0, 10_000, 1_000):
        aimd.on_rate_limited(t)
    assert aimd.effective_rps == 30


# ---------------------------------------------------------------- retry budget / deadline


def test_retry_budget_caps_ratio() -> None:
    budget = RetryBudget(ratio=0.1, min_per_s=0, window_s=10)
    for _ in range(100):
        budget.record_request(1)
    granted = sum(budget.try_spend(1) for _ in range(50))
    assert granted == 10


def test_retry_budget_floor_and_window() -> None:
    budget = RetryBudget(ratio=0.1, min_per_s=1, window_s=10)
    assert sum(budget.try_spend(1) for _ in range(20)) == 10   # floor: 1/s * 10s
    assert not budget.try_spend(5)
    assert budget.try_spend(11)                                # old retries left the window


def test_deadline_caps_attempt_timeout() -> None:
    now = [100.0]
    deadline = Deadline(10.0, started_at=99.0, clock=lambda: now[0])
    assert deadline.remaining() == pytest.approx(9.0)
    assert deadline.attempt_timeout(3.0) == 3.0
    now[0] = 108.5
    assert deadline.attempt_timeout(3.0) == pytest.approx(0.5)
    now[0] = 120.0
    assert deadline.expired() and deadline.remaining() == 0.0
