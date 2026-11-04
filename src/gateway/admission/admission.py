"""Admission control: everything that can refuse a request happens before any work.

1. Load shedding        local in-flight cap, no I/O at all       -> 503 + Retry-After
2. Inbound rate limit   per API key, global (Redis token bucket)  -> 429 + Retry-After
3. Idempotency          Redis SET NX "pending" + GET              -> replay / 409 / 422

(2) and (3) share ONE pipelined Redis round-trip.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import StrEnum

import orjson
import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from gateway.observability import metrics
from gateway.resilience.coordinator import Coordinator, now_ms
from gateway.resilience.ratelimit import LocalTokenBucket
from gateway.resilience.scripts import LuaScript, ScriptPipeline
from gateway.settings import Settings

log = structlog.get_logger(__name__)


class Verdict(StrEnum):
    PROCEED = "proceed"
    THROTTLED = "throttled"                       # inbound rate limit
    REPLAY = "replay"                             # completed earlier: serve cached result
    IN_PROGRESS = "idempotency_in_progress"       # same key still being processed
    KEY_REUSED = "idempotency_key_reused"         # same key, different body
    UNAVAILABLE = "idempotency_unavailable"       # cannot honour the key right now


@dataclass(frozen=True, slots=True)
class Decision:
    verdict: Verdict
    retry_after_s: int = 0
    owns_key: bool = False
    cached_status: int = 0
    cached_body: bytes = b""


def fingerprint(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()[:32]


def tenant_of(api_key: str | None) -> str:
    # Never store or key on raw credentials.
    if not api_key:
        return "anonymous"
    return "k_" + hashlib.sha256(api_key.encode()).hexdigest()[:12]


class Admission:
    def __init__(self, redis: Redis, coordinator: Coordinator, settings: Settings) -> None:
        self._redis = redis
        self._coordinator = coordinator
        self._settings = settings
        self._bucket = LuaScript(redis, "token_bucket.lua")
        self._local: dict[str, LocalTokenBucket] = {}
        self._pending_ttl_ms = settings.request_deadline_ms + 5_000

    @staticmethod
    def _idem_key(tenant: str, key: str) -> str:
        return f"gw:idem:{{{tenant}}}:{key}"

    async def admit(self, tenant: str, idem_key: str | None, body_fp: str | None) -> Decision:
        s = self._settings
        if self._coordinator.degraded:
            return self._admit_degraded(tenant, idem_key)

        pending = orjson.dumps({"state": "pending", "fp": body_fp})
        sp = ScriptPipeline(self._redis)
        sp.script(self._bucket, [f"gw:in:{{{tenant}}}"], [s.inbound_rate_rps, s.inbound_burst, 1])
        if idem_key:
            key = self._idem_key(tenant, idem_key)
            sp.pipe.set(key, pending, nx=True, px=self._pending_ttl_ms)
            sp.pipe.get(key)
        try:
            results = await sp.execute()
        except (RedisError, OSError):
            return self._admit_degraded(tenant, idem_key)

        allowed, retry_after_ms = results[0]
        owns = bool(idem_key) and bool(results[1])
        if not allowed:
            if owns and idem_key:
                await self.release(tenant, idem_key)
            metrics.ADMISSION_REJECTED.labels("rate_limit").inc()
            return Decision(Verdict.THROTTLED, retry_after_s=max(1, math.ceil(retry_after_ms / 1000)))
        if not idem_key or owns:
            return Decision(Verdict.PROCEED, owns_key=owns)

        stored = results[2]
        if stored is None:  # expired between SET and GET: vanishingly rare, just proceed
            return Decision(Verdict.PROCEED)
        record = orjson.loads(stored)
        if record.get("fp") != body_fp:
            metrics.ADMISSION_REJECTED.labels("idempotency_key_reused").inc()
            return Decision(Verdict.KEY_REUSED)
        if record.get("state") == "pending":
            metrics.ADMISSION_REJECTED.labels("idempotency_in_progress").inc()
            return Decision(Verdict.IN_PROGRESS, retry_after_s=1)
        metrics.IDEMPOTENT_REPLAYS.inc()
        return Decision(
            Verdict.REPLAY,
            cached_status=int(record["status"]),
            cached_body=record["body"].encode(),
        )

    def _admit_degraded(self, tenant: str, idem_key: str | None) -> Decision:
        if idem_key:
            # The caller asked for exactly-once; without Redis we cannot promise it.
            # Fail closed for these only — keyless traffic keeps flowing.
            metrics.ADMISSION_REJECTED.labels("idempotency_unavailable").inc()
            return Decision(Verdict.UNAVAILABLE, retry_after_s=1)
        s = self._settings
        share = max(1, self._coordinator.replicas)
        bucket = self._local.get(tenant)
        if bucket is None:
            bucket = self._local[tenant] = LocalTokenBucket(
                s.inbound_rate_rps / share, max(1.0, s.inbound_burst / share), now_ms()
            )
        allowed, retry_after_ms = bucket.acquire(now_ms())
        if not allowed:
            metrics.ADMISSION_REJECTED.labels("rate_limit").inc()
            return Decision(Verdict.THROTTLED, retry_after_s=max(1, math.ceil(retry_after_ms / 1000)))
        return Decision(Verdict.PROCEED)

    async def complete(
        self, tenant: str, idem_key: str, body_fp: str, status: int, body: bytes
    ) -> None:
        """Cache a final result so a retry with the same key replays it."""
        record = orjson.dumps(
            {"state": "done", "fp": body_fp, "status": status, "body": body.decode()}
        )
        try:
            await self._redis.set(
                self._idem_key(tenant, idem_key), record, ex=self._settings.idempotency_ttl_s
            )
        except (RedisError, OSError) as exc:
            log.warning("idempotency_store_failed", error=repr(exc))

    async def release(self, tenant: str, idem_key: str) -> None:
        """Retryable failure: drop the pending marker so the caller may try again."""
        try:
            await self._redis.delete(self._idem_key(tenant, idem_key))
        except (RedisError, OSError) as exc:
            log.warning("idempotency_release_failed", error=repr(exc))
