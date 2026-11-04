"""Cross-replica coordination through Redis, with a degrade-open fallback.

Request path cost per upstream attempt: ONE Redis call (the outbound token bucket),
because a provider's rate limit is global and cannot be enforced locally. Everything
else — breaker state, effective rate (AIMD), 429 back-off windows, replica count — is
read from a local cache refreshed every `state_refresh_ms` by a single pipelined
round-trip. 3 providers x 10k RPS would otherwise be 30k Redis ops/s for data that
does not need millisecond freshness.

If Redis is unreachable we do not reject traffic: each replica falls back to local
breakers and local token buckets sized `effective_rps / replicas`. Availability over
perfect coordination; the condition is exported as a metric and logged loudly.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from gateway.observability import metrics
from gateway.resilience.breaker import BreakerPolicy, BreakerState, LocalBreaker, RollingWindow
from gateway.resilience.outcomes import BREAKER_FAILURES, Outcome
from gateway.resilience.ratelimit import AimdState, LocalTokenBucket
from gateway.resilience.scripts import LuaScript, ScriptPipeline
from gateway.settings import ProviderConfig, Settings

log = structlog.get_logger(__name__)

REPLICAS_KEY = "gw:replicas"
REPLICA_TTL_MS = 5_000


def now_ms() -> float:
    return time.time() * 1000.0


class _Keys:
    """Hash-tagged per provider so every script's keys share one Redis Cluster slot."""

    __slots__ = ("aimd", "bucket", "counts", "probe", "state")

    def __init__(self, provider: str) -> None:
        tag = f"gw:{{prov:{provider}}}"
        self.bucket = f"{tag}:bucket"
        self.aimd = f"{tag}:aimd"
        self.state = f"{tag}:cb"
        self.counts = f"{tag}:cb:counts"
        self.probe = f"{tag}:cb:probe"


@dataclass(slots=True)
class ProviderHealth:
    """This replica's view of one provider's shared state, plus local fallbacks."""

    config: ProviderConfig
    keys: _Keys
    local_breaker: LocalBreaker
    local_aimd: AimdState
    local_bucket: LocalTokenBucket
    breaker_state: BreakerState = BreakerState.CLOSED
    effective_rps: float = 0.0
    blocked_until_ms: float = 0.0
    # accumulated between refreshes, flushed to Redis
    pending_counts: RollingWindow = field(default_factory=lambda: RollingWindow(3600))
    pending_decrease: bool = False
    pending_retry_after_ms: int = 0
    pending_probe_ok: bool | None = None
    decreased_since_refresh: bool = False
    probe_denied_until_ms: float = 0.0

    @property
    def name(self) -> str:
        return self.config.name


class Coordinator:
    def __init__(self, redis: Redis, settings: Settings, providers: list[ProviderConfig]) -> None:
        self._redis = redis
        self._settings = settings
        self._policy = BreakerPolicy(
            window_s=settings.breaker_window_s,
            failure_ratio=settings.breaker_failure_ratio,
            min_volume=settings.breaker_min_volume,
            cooldown_ms=settings.breaker_cooldown_ms,
        )
        self.replica_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.replicas = 1
        self.degraded = False
        self._task: asyncio.Task[None] | None = None
        self._token_bucket = LuaScript(redis, "token_bucket.lua")
        self._aimd = LuaScript(redis, "aimd.lua")
        self._breaker = LuaScript(redis, "breaker.lua")

        t = now_ms()
        self.providers: dict[str, ProviderHealth] = {}
        for cfg in providers:
            aimd = AimdState(
                max_rps=cfg.rate_limit_rps,
                min_rps=cfg.rate_limit_min_rps,
                factor=settings.aimd_decrease_factor,
                step=cfg.rate_limit_rps * settings.aimd_increase_ratio,
                window_ms=settings.aimd_window_ms,
            )
            self.providers[cfg.name] = ProviderHealth(
                config=cfg,
                keys=_Keys(cfg.name),
                local_breaker=LocalBreaker(self._policy),
                local_aimd=aimd,
                local_bucket=LocalTokenBucket(cfg.rate_limit_rps, cfg.rate_limit_burst, t),
                effective_rps=cfg.rate_limit_rps,
            )

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        await self.refresh_once()
        self._task = asyncio.create_task(self._refresh_loop(), name="coordinator-refresh")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        with contextlib.suppress(RedisError, OSError):
            await self._redis.zrem(REPLICAS_KEY, self.replica_id)

    async def _refresh_loop(self) -> None:
        period = self._settings.state_refresh_ms / 1000.0
        while True:
            await asyncio.sleep(period)
            try:
                await self.refresh_once()
            except Exception:  # never let the loop die
                log.exception("coordinator_refresh_crashed")

    # ---------------------------------------------------------------- request path

    def breaker_state(self, p: ProviderHealth) -> BreakerState:
        return p.local_breaker.state if self.degraded else p.breaker_state

    def is_eligible(self, p: ProviderHealth, now: float) -> bool:
        """OPEN and 429-blocked providers are skipped. HALF_OPEN needs a probe slot."""
        return p.blocked_until_ms <= now and self.breaker_state(p) is not BreakerState.OPEN

    async def try_acquire_probe(self, p: ProviderHealth) -> bool:
        if self.degraded:
            return p.local_breaker.try_acquire_probe()
        now = now_ms()
        if p.probe_denied_until_ms > now:
            return False
        ttl = p.config.timeout_ms + 2_000
        try:
            acquired = await self._redis.set(p.keys.probe, self.replica_id, nx=True, px=ttl)
        except (RedisError, OSError) as exc:
            self._enter_degraded(exc)
            return p.local_breaker.try_acquire_probe()
        if acquired:
            return True
        p.probe_denied_until_ms = now + self._settings.state_refresh_ms
        return False

    def release_probe(self, p: ProviderHealth) -> None:
        """We won the probe slot but then could not send (e.g. no token)."""
        if self.degraded:
            p.local_breaker.probe_result(now_ms(), success=False)
            return
        task = asyncio.create_task(self._safe(self._redis.delete(p.keys.probe)))
        _BACKGROUND.add(task)
        task.add_done_callback(_BACKGROUND.discard)

    async def acquire_token(self, p: ProviderHealth) -> tuple[bool, int]:
        """Outbound rate limit. Global across replicas, so this one must hit Redis."""
        if not self.degraded:
            try:
                allowed, retry_after = await self._token_bucket(
                    [p.keys.bucket], [p.effective_rps, p.config.rate_limit_burst, 1]
                )
                return bool(allowed), int(retry_after)
            except (RedisError, OSError) as exc:
                self._enter_degraded(exc)
        return p.local_bucket.acquire(now_ms())

    def record(
        self,
        p: ProviderHealth,
        outcome: Outcome,
        *,
        retry_after_ms: int = 0,
        probe: bool = False,
        blame_provider: bool = True,
    ) -> None:
        """`blame_provider=False`: the failure says more about us than about the
        provider (our loop was stalled, or the caller's deadline cut the attempt
        short), so it must not push the provider's breaker toward open."""
        now = now_ms()
        if outcome is Outcome.RATE_LIMITED:
            self._on_rate_limited(p, now, retry_after_ms)
        elif blame_provider or outcome not in BREAKER_FAILURES:
            failed = outcome in BREAKER_FAILURES
            p.pending_counts.add(int(now // 1000), failed)
            p.local_breaker.record(now, failed)
        if probe:
            ok = outcome not in BREAKER_FAILURES
            if self.degraded:
                p.local_breaker.probe_result(now, ok)
            else:
                p.pending_probe_ok = ok

    def _on_rate_limited(self, p: ProviderHealth, now: float, retry_after_ms: int) -> None:
        # React locally at once; make it global on the next refresh.
        p.local_aimd.on_rate_limited(now)
        if not p.decreased_since_refresh:
            p.effective_rps = max(
                p.config.rate_limit_min_rps, p.effective_rps * self._settings.aimd_decrease_factor
            )
            p.decreased_since_refresh = True
        if retry_after_ms > 0:
            p.blocked_until_ms = max(p.blocked_until_ms, now + retry_after_ms)
        p.pending_decrease = True
        p.pending_retry_after_ms = max(p.pending_retry_after_ms, retry_after_ms)

    # ---------------------------------------------------------------- refresh

    async def refresh_once(self) -> None:
        now = now_ms()
        for p in self.providers.values():
            p.local_breaker.tick(now)
            p.local_aimd.tick(now)

        s = self._settings
        sp = ScriptPipeline(self._redis)
        pipe = sp.pipe
        handlers: list[tuple[ProviderHealth, str] | None] = []

        for p in self.providers.values():
            for second, (total, failures) in p.pending_counts.drain().items():
                pipe.hincrby(p.keys.counts, f"{second}:t", total)
                handlers.append(None)
                if failures:
                    pipe.hincrby(p.keys.counts, f"{second}:f", failures)
                    handlers.append(None)
            pipe.pexpire(p.keys.counts, s.breaker_window_s * 2_000)
            handlers.append(None)

            breaker_args = [s.breaker_window_s, s.breaker_failure_ratio, s.breaker_min_volume,
                            s.breaker_cooldown_ms]
            breaker_keys = [p.keys.state, p.keys.counts, p.keys.probe]
            if p.pending_probe_ok is not None:
                mode = "probe_ok" if p.pending_probe_ok else "probe_fail"
                sp.script(self._breaker, breaker_keys, [mode, *breaker_args])
                handlers.append(None)
            sp.script(self._breaker, breaker_keys, ["tick", *breaker_args])
            handlers.append((p, "breaker"))

            aimd_args = [p.config.rate_limit_rps, p.config.rate_limit_min_rps,
                         s.aimd_decrease_factor, p.config.rate_limit_rps * s.aimd_increase_ratio,
                         s.aimd_window_ms]
            if p.pending_decrease:
                sp.script(self._aimd, [p.keys.aimd],
                          ["decrease", *aimd_args, p.pending_retry_after_ms])
                handlers.append(None)
            sp.script(self._aimd, [p.keys.aimd], ["tick", *aimd_args, 0])
            handlers.append((p, "aimd"))

            p.pending_decrease = False
            p.pending_retry_after_ms = 0
            p.pending_probe_ok = None

        pipe.zadd(REPLICAS_KEY, {self.replica_id: now})
        pipe.zremrangebyscore(REPLICAS_KEY, 0, now - REPLICA_TTL_MS)
        pipe.zcard(REPLICAS_KEY)
        handlers.extend([None, None, None])

        try:
            results = await sp.execute()
        except (RedisError, OSError) as exc:
            self._enter_degraded(exc)
            self._apply_local_rates()
            return

        self._leave_degraded()
        self.replicas = max(1, int(results[-1]))
        received = now_ms()
        for handler, result in zip(handlers, results, strict=True):
            if handler is None:
                continue
            p, kind = handler
            if kind == "breaker":
                p.breaker_state = BreakerState.parse(result[0])
                if p.breaker_state is not BreakerState.HALF_OPEN:
                    p.probe_denied_until_ms = 0.0
            else:
                p.effective_rps = float(result[0])
                p.decreased_since_refresh = False
                p.blocked_until_ms = max(p.blocked_until_ms, received + int(result[1]))
        self._apply_local_rates()
        self._export_gauges()

    def _apply_local_rates(self) -> None:
        # Fallback buckets are pre-sized to this replica's share of the global limit.
        share = max(1, self.replicas)
        for p in self.providers.values():
            rps = p.local_aimd.effective_rps if self.degraded else p.effective_rps
            p.local_bucket.set_rate(rps / share)
        if self.degraded:
            self._export_gauges()

    def _export_gauges(self) -> None:
        for p in self.providers.values():
            metrics.BREAKER_STATE.labels(p.name).set(int(self.breaker_state(p)))
            rps = p.local_aimd.effective_rps if self.degraded else p.effective_rps
            metrics.PROVIDER_EFFECTIVE_RPS.labels(p.name).set(rps)
        metrics.COORDINATION_DEGRADED.set(1 if self.degraded else 0)

    def _enter_degraded(self, exc: BaseException) -> None:
        if not self.degraded:
            self.degraded = True
            metrics.COORDINATION_DEGRADED.set(1)
            log.error("redis_unreachable_degrading_open", error=repr(exc),
                      replicas_assumed=self.replicas)

    def _leave_degraded(self) -> None:
        if self.degraded:
            self.degraded = False
            metrics.COORDINATION_DEGRADED.set(0)
            log.warning("redis_recovered_coordination_restored")

    @staticmethod
    async def _safe(coro: Any) -> None:
        with contextlib.suppress(RedisError, OSError):
            await coro

    # ---------------------------------------------------------------- introspection

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now = now_ms()
        out: dict[str, dict[str, Any]] = {}
        for p in self.providers.values():
            out[p.name] = {
                "breaker": self.breaker_state(p).name.lower(),
                "effective_rps": round(
                    p.local_aimd.effective_rps if self.degraded else p.effective_rps, 2
                ),
                "configured_rps": p.config.rate_limit_rps,
                "blocked_for_ms": max(0, int(p.blocked_until_ms - now)),
            }
        return out


_BACKGROUND: set[asyncio.Task[None]] = set()
