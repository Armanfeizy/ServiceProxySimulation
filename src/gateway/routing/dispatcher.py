"""The attempt loop: select -> call -> classify -> (maybe) retry elsewhere."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import structlog

from gateway.observability import metrics, tracing
from gateway.observability.looplag import LoopLagMonitor
from gateway.providers.base import CanonicalRequest
from gateway.providers.registry import ProviderRuntime
from gateway.providers.transport import TransportError
from gateway.resilience.coordinator import Coordinator
from gateway.resilience.outcomes import BREAKER_FAILURES, Outcome, is_retryable
from gateway.resilience.retry import RetryBudget
from gateway.routing.deadline import Deadline
from gateway.routing.selector import Selector
from gateway.settings import Settings

log = structlog.get_logger(__name__)


class DispatchStatus(StrEnum):
    OK = "ok"
    CLIENT_ERROR = "client_error"            # provider rejected the caller's input (4xx)
    PROVIDER_ERROR = "provider_error"        # upstream failed, retries exhausted/unsafe
    RATE_LIMITED = "rate_limited"            # every provider's outbound budget is spent
    UNAVAILABLE = "unavailable"              # breakers open / bulkheads full
    DEADLINE_EXCEEDED = "deadline_exceeded"  # caller's time budget ran out


@dataclass(slots=True)
class AttemptRecord:
    provider: str
    attempt: int
    outcome: Outcome
    latency_ms: float
    status_code: int | None = None
    error: str | None = None
    response: Any = None
    response_size: int = 0
    is_probe: bool = False


@dataclass(slots=True)
class DispatchResult:
    status: DispatchStatus
    http_status: int
    provider: str | None = None
    data: Any = None
    attempts: list[AttemptRecord] = field(default_factory=list)
    retry_after_s: int | None = None
    error_class: str | None = None

    @property
    def provider_ms(self) -> float | None:
        return self.attempts[-1].latency_ms if self.attempts else None


class Dispatcher:
    def __init__(
        self,
        selector: Selector,
        coordinator: Coordinator,
        budget: RetryBudget,
        settings: Settings,
        loop_lag: LoopLagMonitor | None = None,
    ) -> None:
        self._selector = selector
        self._coordinator = coordinator
        self._budget = budget
        self._settings = settings
        self._loop_lag = loop_lag
        self._min_attempt_s = settings.min_attempt_ms / 1000.0

    async def dispatch(self, request: CanonicalRequest, deadline: Deadline) -> DispatchResult:
        attempts: list[AttemptRecord] = []
        excluded: set[str] = set()
        self._budget.record_request(int(time.time()))

        while True:
            if deadline.remaining() < self._min_attempt_s:
                return self._failed(attempts, deadline_hit=True)

            selection = await self._selector.select(excluded)
            runtime = selection.provider
            if runtime is None:
                if attempts:
                    return self._failed(attempts)
                metrics.UPSTREAM_UNAVAILABLE.labels(selection.reason).inc()
                status = (
                    DispatchStatus.RATE_LIMITED
                    if selection.reason == "rate_limited"
                    else DispatchStatus.UNAVAILABLE
                )
                return DispatchResult(
                    status=status,
                    http_status=503,
                    retry_after_s=max(1, math.ceil(selection.retry_after_ms / 1000)),
                    error_class=selection.reason,
                )

            record = await self._attempt(runtime, request, deadline, len(attempts) + 1,
                                         selection.is_probe)
            attempts.append(record)

            if record.outcome is Outcome.SUCCESS:
                return DispatchResult(DispatchStatus.OK, 200, runtime.name, record.response, attempts)
            if record.outcome is Outcome.CLIENT_ERROR:
                return DispatchResult(
                    DispatchStatus.CLIENT_ERROR, record.status_code or 400, runtime.name,
                    record.response, attempts, error_class=record.outcome.value,
                )

            excluded.add(runtime.name)
            replay_safe = self._settings.operation_idempotent or (
                runtime.config.supports_idempotency_key and request.idempotency_key is not None
            )
            if len(attempts) >= self._settings.max_attempts:
                return self._failed(attempts)
            if not is_retryable(record.outcome, replay_safe=replay_safe):
                return self._failed(attempts)
            if not self._budget.try_spend(int(time.time())):
                metrics.RETRY_BUDGET_EXHAUSTED.inc()
                return self._failed(attempts)
            metrics.RETRIES.labels(runtime.name).inc()

    async def _attempt(
        self,
        runtime: ProviderRuntime,
        request: CanonicalRequest,
        deadline: Deadline,
        attempt_no: int,
        is_probe: bool,
    ) -> AttemptRecord:
        cfg = runtime.config
        provider_timeout_s = cfg.timeout_ms / 1000.0
        timeout_s = deadline.attempt_timeout(provider_timeout_s)
        upstream = runtime.adapter.build_request(request, attempt_no)
        record = AttemptRecord(runtime.name, attempt_no, Outcome.TIMEOUT, 0.0, is_probe=is_probe)
        retry_after_ms = 0

        with tracing.client_span(runtime.name, cfg.url, attempt_no) as span:
            tracing.inject(upstream.headers)
            started = time.perf_counter()
            try:
                response = await runtime.transport.post(
                    cfg.url,
                    upstream.content,
                    upstream.headers,
                    timeout_s=timeout_s,
                    connect_timeout_s=min(cfg.connect_timeout_ms / 1000.0, timeout_s),
                )
                result = runtime.adapter.parse_response(
                    response.status, response.headers, response.content
                )
                record.outcome = result.outcome
                record.status_code = result.status_code
                record.response = result.data
                record.response_size = len(response.content)
                retry_after_ms = result.retry_after_ms
            except TransportError as exc:  # classified by the transport, never propagated
                record.outcome = exc.outcome
                record.error = exc.detail
            except asyncio.CancelledError:
                if is_probe:
                    self._coordinator.release_probe(runtime.health)
                raise
            finally:
                runtime.release_slot()
            record.latency_ms = (time.perf_counter() - started) * 1000.0
            tracing.finish_client_span(span, record.outcome.value, record.status_code)

        failed = record.outcome in BREAKER_FAILURES
        runtime.observe(record.latency_ms, failed=failed)
        self._coordinator.record(
            runtime.health, record.outcome, retry_after_ms=retry_after_ms, probe=is_probe,
            blame_provider=not self._our_fault(record.outcome, timeout_s, provider_timeout_s),
        )
        outcome = record.outcome.value
        metrics.PROVIDER_ATTEMPTS.labels(runtime.name, outcome).inc()
        metrics.PROVIDER_ATTEMPT_DURATION.labels(runtime.name, outcome).observe(
            record.latency_ms / 1000.0
        )
        if record.outcome is not Outcome.SUCCESS:
            log.warning(
                "upstream_attempt_failed",
                provider=runtime.name,
                attempt=attempt_no,
                outcome=outcome,
                status_code=record.status_code,
                latency_ms=round(record.latency_ms, 1),
                probe=is_probe,
                error=record.error,
            )
        return record

    def _our_fault(self, outcome: Outcome, timeout_s: float, provider_timeout_s: float) -> bool:
        """A timeout is evidence against the provider only if we gave it its full
        timeout and our own event loop was keeping up. Otherwise a saturated replica
        (or a retry squeezed into the last second of a deadline) would open breakers
        on healthy providers: a cascade we caused ourselves."""
        if outcome is not Outcome.TIMEOUT:
            return False
        if timeout_s < 0.9 * provider_timeout_s:
            return True
        return self._loop_lag is not None and self._loop_lag.lagging

    def _failed(self, attempts: list[AttemptRecord], *, deadline_hit: bool = False) -> DispatchResult:
        last = attempts[-1] if attempts else None
        provider = last.provider if last else None
        if deadline_hit or (last is not None and last.outcome is Outcome.TIMEOUT):
            return DispatchResult(
                DispatchStatus.DEADLINE_EXCEEDED, 504, provider, None, attempts,
                error_class=last.outcome.value if last else "deadline",
            )
        assert last is not None
        if last.outcome is Outcome.RATE_LIMITED:
            return DispatchResult(
                DispatchStatus.RATE_LIMITED, 503, provider, None, attempts, retry_after_s=1,
                error_class=last.outcome.value,
            )
        return DispatchResult(
            DispatchStatus.PROVIDER_ERROR, 502, provider, None, attempts,
            error_class=last.outcome.value,
        )
