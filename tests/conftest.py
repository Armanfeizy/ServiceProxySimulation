from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

import pytest
from redis.asyncio import Redis

from gateway.providers.base import build_adapter
from gateway.providers.registry import ProviderRegistry, ProviderRuntime
from gateway.providers.transport import RawResponse, TransportError
from gateway.resilience.coordinator import Coordinator
from gateway.resilience.outcomes import Outcome
from gateway.settings import ProviderConfig, Settings


@dataclass(frozen=True, slots=True)
class SentRequest:
    url: str
    content: bytes
    headers: Mapping[str, str]


Handler = Callable[[SentRequest], Awaitable[RawResponse]]


class FakeTransport:
    """In-memory provider: same timeout semantics as AiohttpTransport."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler

    async def post(
        self, url: str, content: bytes, headers: Mapping[str, str], *,
        timeout_s: float, connect_timeout_s: float,
    ) -> RawResponse:
        try:
            async with asyncio.timeout(timeout_s):
                return await self._handler(SentRequest(url, content, headers))
        except TimeoutError as exc:
            raise TransportError(Outcome.TIMEOUT, "TimeoutError") from exc

    async def aclose(self) -> None:
        return None


def provider_config(name: str, **overrides: object) -> ProviderConfig:
    base: dict[str, object] = {
        "name": name,
        "url": f"http://mock-{name.lower()}/v1/process",
        "expected_latency_ms": 100,
        "connect_timeout_ms": 200,
        "timeout_ms": 1000,
        "rate_limit_rps": 1000,
        "rate_limit_min_rps": 10,
        "rate_limit_burst": 100,
        "max_concurrency": 100,
    }
    base.update(overrides)
    return ProviderConfig.model_validate(base)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        request_deadline_ms=2000,
        min_attempt_ms=50,
        retry_budget_min_per_s=100,
        otel_enabled=False,
        audit_enabled=False,
    )


def offline_coordinator(settings: Settings, configs: list[ProviderConfig]) -> Coordinator:
    """A real Coordinator in degrade-open mode: all state local, no Redis I/O."""
    coordinator = Coordinator(Redis(host="127.0.0.1", port=1), settings, configs)
    coordinator.degraded = True
    return coordinator


def build_registry(
    settings: Settings, coordinator: Coordinator, configs: list[ProviderConfig],
    handlers: dict[str, Handler],
) -> ProviderRegistry:
    runtimes = [
        ProviderRuntime(
            config=cfg,
            adapter=build_adapter(cfg),
            transport=FakeTransport(handlers[cfg.name]),
            health=coordinator.providers[cfg.name],
            alpha=settings.ewma_alpha,
        )
        for cfg in configs
    ]
    return ProviderRegistry(runtimes)
