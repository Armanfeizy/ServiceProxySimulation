"""Operational endpoints: health, readiness, metrics, provider state, audit lookup."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import ORJSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError

from gateway.api.container import Gateway

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the event loop is responsive."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> ORJSONResponse:
    """Readiness: started and not draining.

    Dependencies are *reported*, not required. Redis down means degrade-open and Kafka
    down means audit drops — both by design. Failing readiness on them would let an
    audit-path outage take the whole serving path down, the cascade we exist to prevent.
    """
    gw: Gateway | None = getattr(request.app.state, "gw", None)
    if gw is None or not gw.ready:
        return ORJSONResponse({"status": "starting_or_draining"}, status_code=503)
    redis_ok = True
    try:
        async with asyncio.timeout(0.25):
            await gw.redis.ping()
    except (RedisError, OSError, TimeoutError):
        redis_ok = False
    return ORJSONResponse({
        "status": "ready",
        "redis": "ok" if redis_ok else "unreachable",
        "coordination": "degraded_local_limits" if gw.coordinator.degraded else "shared",
        "audit_pipeline": ("connected" if gw.audit.connected else "disconnected")
        if gw.settings.audit_enabled else "disabled",
    })


@router.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/v1/providers")
async def providers(request: Request) -> dict[str, Any]:
    """Live provider state as seen by *this* replica (shared state + local stats)."""
    gw: Gateway = request.app.state.gw
    shared = gw.coordinator.snapshot()
    out: dict[str, Any] = {}
    for runtime in gw.registry.all():
        out[runtime.name] = {
            **shared[runtime.name],
            "url": runtime.config.url,
            "ewma_latency_ms": round(runtime.ewma_ms, 1),
            "inflight": runtime.inflight,
            "max_concurrency": runtime.config.max_concurrency,
            "score": round(runtime.score(), 1),
        }
    return {
        "replica": gw.coordinator.replica_id,
        "replicas_seen": gw.coordinator.replicas,
        "coordination": "degraded_local_limits" if gw.coordinator.degraded else "shared",
        "inflight_requests": gw.inflight,
        "event_loop_lag_ms": round(gw.loop_lag.lag_ms, 1),
        "audit_buffer_depth": gw.audit.depth(),
        "providers": out,
    }


@router.get("/v1/requests/{trace_id}")
async def lookup(trace_id: str, request: Request) -> ORJSONResponse:
    gw: Gateway = request.app.state.gw
    try:
        record = await gw.lookup.get(trace_id)
    except Exception as exc:  # the audit store being down must not look like a 500 bug
        return ORJSONResponse(
            {"error": {"code": "audit_store_unavailable", "message": type(exc).__name__}},
            status_code=503,
        )
    if record is None:
        return ORJSONResponse(
            {"error": {"code": "not_found",
                       "message": "unknown trace_id (or not persisted yet - the audit path is async)"}},
            status_code=404,
        )
    return ORJSONResponse(record)
