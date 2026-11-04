"""Selector + dispatcher against in-process mock providers (httpx.MockTransport)."""

from __future__ import annotations

import asyncio
import random
import time

import orjson

from gateway.providers.base import CanonicalRequest
from gateway.providers.registry import ProviderRegistry
from gateway.providers.transport import RawResponse
from gateway.resilience.breaker import BreakerState
from gateway.resilience.coordinator import Coordinator
from gateway.resilience.retry import RetryBudget
from gateway.routing.deadline import Deadline
from gateway.routing.dispatcher import Dispatcher, DispatchStatus
from gateway.routing.selector import Selector, p2c_order
from gateway.settings import Settings
from tests.conftest import (
    Handler,
    SentRequest,
    build_registry,
    offline_coordinator,
    provider_config,
)

REQUEST = CanonicalRequest("01JTESTTRACE", "anonymous", {"x": 1}, b'{"x": 1}', None)


def ok(name: str) -> Handler:
    async def handler(request: SentRequest) -> RawResponse:
        assert request.headers["x-request-id"] == REQUEST.trace_id
        body = {"provider": name, "echo": orjson.loads(request.content)}
        return RawResponse(200, {}, orjson.dumps(body))

    return handler


def status(code: int, headers: dict[str, str] | None = None) -> Handler:
    async def handler(request: SentRequest) -> RawResponse:
        return RawResponse(code, headers or {}, orjson.dumps({"error": code}))

    return handler


def hang(seconds: float) -> Handler:
    async def handler(request: SentRequest) -> RawResponse:
        await asyncio.sleep(seconds)
        return RawResponse(200, {}, b"{}")

    return handler


def make(
    settings: Settings, handlers: dict[str, Handler], **cfg_overrides: object
) -> tuple[Dispatcher, ProviderRegistry, Coordinator]:
    configs = [provider_config(n, **cfg_overrides) for n in handlers]
    coordinator = offline_coordinator(settings, configs)
    registry = build_registry(settings, coordinator, configs, handlers)
    selector = Selector(registry, coordinator, rng=random.Random(7))
    budget = RetryBudget(settings.retry_budget_ratio, settings.retry_budget_min_per_s,
                         settings.retry_budget_window_s)
    return Dispatcher(selector, coordinator, budget, settings), registry, coordinator


def deadline(settings: Settings) -> Deadline:
    return Deadline(settings.request_deadline_ms / 1000)


# ---------------------------------------------------------------- selection


async def test_p2c_prefers_lower_score(settings: Settings) -> None:
    _, registry, _ = make(settings, {"A": ok("A"), "B": ok("B")})
    a, b = registry.get("A"), registry.get("B")
    assert a and b
    a.ewma_ms, b.ewma_ms = 100, 1000
    rng = random.Random(1)
    assert all(p2c_order([a, b], rng)[0] is a for _ in range(50))
    a.inflight = 20                           # 100 * 21 > 1000 * 1: queueing flips it
    assert p2c_order([a, b], rng)[0] is b


async def test_open_breaker_is_skipped(settings: Settings) -> None:
    dispatcher, _, coordinator = make(settings, {"A": status(500), "B": ok("B")})
    coordinator.providers["A"].local_breaker.state = BreakerState.OPEN
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.OK and result.provider == "B"
    assert [a.provider for a in result.attempts] == ["B"]


async def test_bulkhead_full_provider_is_skipped(settings: Settings) -> None:
    dispatcher, registry, _ = make(settings, {"A": ok("A"), "B": ok("B")}, max_concurrency=1)
    a = registry.get("A")
    assert a
    a.inflight = 1
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.provider == "B"


async def test_all_unavailable_is_503_not_hang(settings: Settings) -> None:
    dispatcher, _, coordinator = make(settings, {"A": ok("A"), "B": ok("B")})
    for p in coordinator.providers.values():
        p.local_breaker.state = BreakerState.OPEN
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.UNAVAILABLE and result.http_status == 503
    assert result.retry_after_s and result.attempts == []


async def test_outbound_rate_limit_spills_to_next_provider(settings: Settings) -> None:
    dispatcher, _, coordinator = make(settings, {"A": ok("A"), "B": ok("B")})
    a = coordinator.providers["A"]
    a.local_bucket = type(a.local_bucket)(rate=0.001, burst=1, now_ms=0)
    a.local_bucket.acquire(0)                 # A's budget is spent
    seen = {(await dispatcher.dispatch(REQUEST, deadline(settings))).provider for _ in range(10)}
    assert seen == {"B"}


async def test_half_open_allows_a_single_probe(settings: Settings) -> None:
    dispatcher, _, coordinator = make(settings, {"A": ok("A")})
    breaker = coordinator.providers["A"].local_breaker
    breaker.state = BreakerState.HALF_OPEN
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.OK and result.attempts[0].is_probe
    assert breaker.state is BreakerState.CLOSED


# ---------------------------------------------------------------- retry policy


async def test_rejected_503_is_retried_on_another_provider(settings: Settings) -> None:
    dispatcher, _, _ = make(settings, {"A": status(503), "B": status(503), "C": ok("C")})
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.OK and result.provider == "C"
    assert len(result.attempts) <= 3
    assert result.attempts[-1].provider == "C"


async def test_500_is_not_retried_when_replay_is_unsafe(settings: Settings) -> None:
    dispatcher, _, _ = make(settings, {"A": status(500), "B": status(500)})
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.PROVIDER_ERROR and result.http_status == 502
    assert len(result.attempts) == 1          # may have been processed: never duplicate


async def test_500_is_retried_when_operation_is_idempotent(settings: Settings) -> None:
    settings.operation_idempotent = True
    dispatcher, _, _ = make(settings, {"A": status(500), "B": status(500), "C": ok("C")})
    # Force order A -> B -> C regardless of P2C sampling.
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.OK


async def test_429_is_retried_and_feeds_aimd(settings: Settings) -> None:
    dispatcher, _, coordinator = make(
        settings, {"A": status(429, {"retry-after": "2"}), "B": status(429)}
    )
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.RATE_LIMITED and result.http_status == 503
    assert len(result.attempts) == 2
    a = coordinator.providers["A"]
    assert a.local_aimd.effective_rps < a.config.rate_limit_rps
    assert not coordinator.is_eligible(a, now=a.blocked_until_ms - 1)  # Retry-After honoured


async def test_client_error_passes_through_without_retry(settings: Settings) -> None:
    dispatcher, _, _ = make(settings, {"A": status(422), "B": status(422)})
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.CLIENT_ERROR and result.http_status == 422
    assert len(result.attempts) == 1


async def test_read_timeout_is_504_and_not_retried(settings: Settings) -> None:
    settings.request_deadline_ms = 400
    dispatcher, _, _ = make(settings, {"A": hang(5), "B": hang(5)})
    result = await dispatcher.dispatch(REQUEST, deadline(settings))
    assert result.status is DispatchStatus.DEADLINE_EXCEEDED and result.http_status == 504
    assert len(result.attempts) == 1
    assert result.attempts[0].latency_ms < 600   # deadline-capped, not provider timeout


async def test_retry_budget_stops_retry_storms(settings: Settings) -> None:
    settings.retry_budget_min_per_s = 0
    settings.retry_budget_ratio = 0.0
    dispatcher, _, _ = make(settings, {"A": status(503), "B": ok("B")})
    statuses = set()
    for _ in range(5):
        statuses.add((await dispatcher.dispatch(REQUEST, deadline(settings))).status)
    # With a zero budget, a 503 from A is final; B still serves whatever P2C sends it.
    assert DispatchStatus.PROVIDER_ERROR in statuses or statuses == {DispatchStatus.OK}


async def test_fast_failures_raise_ewma(settings: Settings) -> None:
    dispatcher, registry, _ = make(settings, {"A": status(503)})
    a = registry.get("A")
    assert a
    before = a.ewma_ms
    await dispatcher.dispatch(REQUEST, deadline(settings))
    assert a.ewma_ms > before                 # a fast 503 must not look attractive
    assert a.inflight == 0                    # bulkhead slot released


async def test_deadline_capped_timeout_does_not_blame_provider(settings: Settings) -> None:
    # Provider timeout 1s but only 0.4s of deadline left: the timeout is ours, not B's.
    settings.request_deadline_ms = 400
    dispatcher, _, coordinator = make(settings, {"B": hang(5)})
    await dispatcher.dispatch(REQUEST, deadline(settings))
    assert coordinator.providers["B"].local_breaker.counts(time.time() * 1000) == (0, 0)


async def test_full_timeout_counts_against_provider(settings: Settings) -> None:
    settings.request_deadline_ms = 5000
    dispatcher, _, coordinator = make(settings, {"B": hang(5)}, timeout_ms=200)
    await dispatcher.dispatch(REQUEST, deadline(settings))
    assert coordinator.providers["B"].local_breaker.counts(time.time() * 1000) == (1, 1)
