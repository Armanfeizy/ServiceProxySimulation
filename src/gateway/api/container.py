"""Wires the long-lived components of one api replica together."""

from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import BlockingConnectionPool, Redis

from gateway.admission.admission import Admission
from gateway.audit.lookup import AuditLookup
from gateway.audit.producer import AuditProducer
from gateway.observability import metrics
from gateway.observability.looplag import LoopLagMonitor
from gateway.providers.registry import ProviderRegistry
from gateway.resilience.coordinator import Coordinator
from gateway.resilience.retry import RetryBudget
from gateway.routing.dispatcher import Dispatcher
from gateway.routing.selector import Selector
from gateway.settings import Settings


def make_redis(settings: Settings) -> Redis:
    timeout = settings.redis_timeout_ms / 1000.0
    pool = BlockingConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_pool_size,
        timeout=timeout,                 # wait for a pooled connection
        socket_timeout=timeout,          # wait for a reply
        socket_connect_timeout=timeout,
        health_check_interval=30,
    )
    return Redis(connection_pool=pool)


@dataclass(slots=True)
class Gateway:
    settings: Settings
    redis: Redis
    coordinator: Coordinator
    registry: ProviderRegistry
    dispatcher: Dispatcher
    admission: Admission
    audit: AuditProducer
    lookup: AuditLookup
    loop_lag: LoopLagMonitor
    inflight: int = 0
    ready: bool = False

    @classmethod
    async def start(cls, settings: Settings) -> Gateway:
        configs = settings.providers()
        redis = make_redis(settings)
        coordinator = Coordinator(redis, settings, configs)
        registry = ProviderRegistry.build(settings, configs, coordinator.providers)
        budget = RetryBudget(
            settings.retry_budget_ratio, settings.retry_budget_min_per_s,
            settings.retry_budget_window_s,
        )
        loop_lag = LoopLagMonitor(settings.max_loop_lag_ms)
        dispatcher = Dispatcher(
            Selector(registry, coordinator), coordinator, budget, settings, loop_lag
        )
        gateway = cls(
            settings=settings,
            redis=redis,
            coordinator=coordinator,
            registry=registry,
            dispatcher=dispatcher,
            admission=Admission(redis, coordinator, settings),
            audit=AuditProducer(settings),
            lookup=AuditLookup(settings),
            loop_lag=loop_lag,
        )
        await loop_lag.start()
        await coordinator.start()     # tolerates Redis being down (starts degraded)
        await gateway.audit.start()   # connects to Kafka in the background
        gateway.ready = True
        return gateway

    async def stop(self) -> None:
        self.ready = False            # /readyz -> 503 so the balancer drains us first
        await self.audit.stop()
        await self.loop_lag.stop()
        await self.coordinator.stop()
        await self.registry.aclose()
        await self.lookup.aclose()
        await self.redis.aclose()

    def enter(self) -> None:
        self.inflight += 1
        metrics.INFLIGHT_REQUESTS.set(self.inflight)

    def leave(self) -> None:
        self.inflight -= 1
        metrics.INFLIGHT_REQUESTS.set(self.inflight)
