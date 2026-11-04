"""Per-provider runtime: adapter + connection pool + local latency stats + bulkhead."""

from __future__ import annotations

from gateway.observability import metrics
from gateway.providers import adapters as _adapters  # noqa: F401  (registers adapters)
from gateway.providers.base import ProviderAdapter, build_adapter
from gateway.providers.transport import AiohttpTransport, Transport
from gateway.resilience.coordinator import ProviderHealth
from gateway.settings import ProviderConfig, Settings


class ProviderRuntime:
    """Everything one replica knows about one provider.

    EWMA latency and in-flight count are purely local: each replica learns from its own
    observations, which costs no coordination and is what P2C needs.
    """

    __slots__ = ("adapter", "alpha", "config", "ewma_ms", "health", "inflight", "transport")

    def __init__(
        self,
        config: ProviderConfig,
        adapter: ProviderAdapter,
        transport: Transport,
        health: ProviderHealth,
        alpha: float,
    ) -> None:
        self.config = config
        self.adapter = adapter
        self.transport = transport
        self.health = health
        self.alpha = alpha
        self.ewma_ms = float(config.expected_latency_ms)
        self.inflight = 0
        metrics.PROVIDER_EWMA_MS.labels(config.name).set(self.ewma_ms)

    @property
    def name(self) -> str:
        return self.config.name

    # --- bulkhead: one slow provider must not absorb the whole event loop ----------

    def has_room(self) -> bool:
        return self.inflight < self.config.max_concurrency

    def acquire_slot(self) -> bool:
        # No await between check and increment: atomic on a single event loop.
        if self.inflight >= self.config.max_concurrency:
            return False
        self.inflight += 1
        metrics.PROVIDER_INFLIGHT.labels(self.name).inc()
        return True

    def release_slot(self) -> None:
        self.inflight -= 1
        metrics.PROVIDER_INFLIGHT.labels(self.name).dec()

    # --- latency model -------------------------------------------------------------

    def observe(self, latency_ms: float, *, failed: bool) -> None:
        sample = latency_ms
        if failed:
            # A provider that fails *fast* must not look attractive to the selector.
            sample = min(max(latency_ms, 2 * self.ewma_ms), float(self.config.timeout_ms))
        self.ewma_ms = self.alpha * sample + (1 - self.alpha) * self.ewma_ms
        metrics.PROVIDER_EWMA_MS.labels(self.name).set(self.ewma_ms)

    def score(self) -> float:
        """Lower is better: expected latency inflated by current queueing."""
        return self.ewma_ms * (1 + self.inflight)


def make_transport(config: ProviderConfig, settings: Settings) -> Transport:
    return AiohttpTransport(config.max_concurrency, proxy=settings.upstream_proxy)


class ProviderRegistry:
    def __init__(self, runtimes: list[ProviderRuntime]) -> None:
        self._by_name = {r.name: r for r in runtimes}

    @classmethod
    def build(
        cls, settings: Settings, configs: list[ProviderConfig], health: dict[str, ProviderHealth]
    ) -> ProviderRegistry:
        runtimes = [
            ProviderRuntime(
                config=cfg,
                adapter=build_adapter(cfg),
                transport=make_transport(cfg, settings),
                health=health[cfg.name],
                alpha=settings.ewma_alpha,
            )
            for cfg in configs
        ]
        return cls(runtimes)

    def get(self, name: str) -> ProviderRuntime | None:
        return self._by_name.get(name)

    def all(self) -> list[ProviderRuntime]:
        return list(self._by_name.values())

    async def aclose(self) -> None:
        for runtime in self._by_name.values():
            await runtime.transport.aclose()
