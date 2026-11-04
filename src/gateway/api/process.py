"""POST /v1/process — the one proxied endpoint."""

from __future__ import annotations

import re
import time
from typing import Any

import orjson
from fastapi import APIRouter, Request
from fastapi.responses import Response

from gateway.admission.admission import Verdict, fingerprint, tenant_of
from gateway.api.container import Gateway
from gateway.audit.events import SCHEMA_VERSION, AttemptDoc, AuditEvent, clip
from gateway.observability import metrics, tracing
from gateway.providers.base import CanonicalRequest
from gateway.routing.deadline import Deadline
from gateway.routing.dispatcher import DispatchResult, DispatchStatus

router = APIRouter()

_VALID_IDEM_KEY = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")

_MESSAGES = {
    DispatchStatus.PROVIDER_ERROR: "upstream provider failed and no safe retry was possible",
    DispatchStatus.DEADLINE_EXCEEDED: "upstream did not answer within the request deadline",
    DispatchStatus.RATE_LIMITED: "upstream capacity is exhausted; retry after the given delay",
    DispatchStatus.UNAVAILABLE: "no upstream provider is currently available",
}


class _Ctx:
    """Per-request facts needed to answer the caller and to audit afterwards."""

    __slots__ = ("client_request_id", "idem_key", "raw_body", "received_at", "started", "tenant",
                 "trace_id")

    def __init__(self, request: Request) -> None:
        state = request.scope["state"]
        self.trace_id: str = state["trace_id"]
        self.client_request_id: str | None = state["client_request_id"]
        self.received_at: float = state["received_at"]
        self.started: float = state["started"]
        self.tenant = tenant_of(request.headers.get("x-api-key"))
        self.idem_key: str | None = request.headers.get("idempotency-key")
        self.raw_body = b""

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started) * 1000.0


def _json(status: int, body: dict[str, Any], headers: dict[str, str] | None = None) -> Response:
    return Response(orjson.dumps(body), status_code=status, headers=headers,
                    media_type="application/json")


def _error(ctx: _Ctx, status: int, code: str, message: str, *,
           retry_after_s: int | None = None, extra: dict[str, Any] | None = None) -> Response:
    error: dict[str, Any] = {"code": code, "message": message, **(extra or {})}
    headers = {"retry-after": str(retry_after_s)} if retry_after_s else None
    return _json(status, {"trace_id": ctx.trace_id, "error": error}, headers)


@router.post("/v1/process")
async def process(request: Request) -> Response:
    gw: Gateway = request.app.state.gw
    ctx = _Ctx(request)

    # 1. Load shedding — before reading a single byte. Fail fast beats fail slow.
    #    Two signals: too many requests in flight, or the event loop running late
    #    (CPU saturation, which in-flight counts cannot see on a slow-upstream proxy).
    shed_reason = None
    if gw.inflight >= gw.settings.max_inflight:
        shed_reason = "shed_inflight"
    elif gw.loop_lag.should_shed():
        shed_reason = "shed_loop_lag"
    if shed_reason:
        metrics.ADMISSION_REJECTED.labels(shed_reason).inc()
        response = _error(ctx, 503, "overloaded", "gateway is at capacity",
                          retry_after_s=gw.settings.shed_retry_after_s)
        _audit(gw, request, ctx, "shed", response.status_code)
        return response

    gw.enter()
    try:
        return await _process(gw, request, ctx)
    finally:
        gw.leave()


async def _process(gw: Gateway, request: Request, ctx: _Ctx) -> Response:
    s = gw.settings

    # 2. Validation (local, cheap) — reject garbage before it costs a Redis call.
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > s.max_body_bytes:
        return _reject(gw, request, ctx, 413, "body_too_large", f"limit is {s.max_body_bytes} bytes")
    ctx.raw_body = await request.body()
    if len(ctx.raw_body) > s.max_body_bytes:
        return _reject(gw, request, ctx, 413, "body_too_large", f"limit is {s.max_body_bytes} bytes")
    try:
        body = orjson.loads(ctx.raw_body)
    except orjson.JSONDecodeError:
        body = None
    if not isinstance(body, dict):
        return _reject(gw, request, ctx, 422, "invalid_body", "body must be a JSON object")
    if ctx.idem_key is not None and not _VALID_IDEM_KEY.match(ctx.idem_key):
        return _reject(gw, request, ctx, 400, "invalid_idempotency_key",
                       "Idempotency-Key must match [A-Za-z0-9._:-]{1,128}")

    # 3. Admission — inbound rate limit + idempotency in one Redis round-trip.
    body_fp = fingerprint(ctx.raw_body) if ctx.idem_key else None
    decision = await gw.admission.admit(ctx.tenant, ctx.idem_key, body_fp)
    if decision.verdict is Verdict.REPLAY:
        response = Response(decision.cached_body, status_code=decision.cached_status,
                            media_type="application/json",
                            headers={"idempotent-replayed": "true"})
        _audit(gw, request, ctx, "replayed", response.status_code)
        return response
    if decision.verdict is not Verdict.PROCEED:
        status, message = {
            Verdict.THROTTLED: (429, "rate limit exceeded for this API key"),
            Verdict.IN_PROGRESS: (409, "a request with this Idempotency-Key is in progress"),
            Verdict.KEY_REUSED: (422, "Idempotency-Key was already used with a different body"),
            Verdict.UNAVAILABLE: (503, "idempotency store unavailable; retry shortly"),
        }[decision.verdict]
        response = _error(ctx, status, decision.verdict.value, message,
                          retry_after_s=decision.retry_after_s or None)
        _audit(gw, request, ctx, decision.verdict.value, status)
        return response

    # 4. Dispatch — selection, upstream call, classification, retries.
    canonical = CanonicalRequest(ctx.trace_id, ctx.tenant, body, ctx.raw_body, ctx.idem_key)
    deadline = Deadline(s.request_deadline_ms / 1000.0, started_at=ctx.started)
    result = await gw.dispatcher.dispatch(canonical, deadline)
    response = _respond(ctx, result)

    # 5. Idempotency bookkeeping: cache final answers, release retryable failures.
    if decision.owns_key and ctx.idem_key and body_fp:
        if result.status in (DispatchStatus.OK, DispatchStatus.CLIENT_ERROR):
            await gw.admission.complete(ctx.tenant, ctx.idem_key, body_fp,
                                        response.status_code, bytes(response.body))
        else:
            await gw.admission.release(ctx.tenant, ctx.idem_key)

    # *** caller is done; everything below is O(1) and never blocks ***
    _audit(gw, request, ctx, result.status.value, response.status_code, result, body)
    return response


def _reject(gw: Gateway, request: Request, ctx: _Ctx, status: int, code: str, message: str) -> Response:
    metrics.ADMISSION_REJECTED.labels(code).inc()
    _audit(gw, request, ctx, code, status)
    return _error(ctx, status, code, message)


def _respond(ctx: _Ctx, result: DispatchResult) -> Response:
    total_ms = round(ctx.elapsed_ms(), 1)
    headers = {"x-attempts": str(len(result.attempts))}
    if result.provider:
        headers["x-provider"] = result.provider

    if result.status is DispatchStatus.OK:
        return _json(200, {
            "trace_id": ctx.trace_id,
            "provider": result.provider,
            "attempts": len(result.attempts),
            "latency_ms": total_ms,
            "result": result.data,
        }, headers)

    attempts = [
        {"provider": a.provider, "outcome": a.outcome.value, "status_code": a.status_code,
         "latency_ms": round(a.latency_ms, 1)}
        for a in result.attempts
    ]
    if result.status is DispatchStatus.CLIENT_ERROR:
        return _json(result.http_status, {
            "trace_id": ctx.trace_id,
            "provider": result.provider,
            "error": {"code": "provider_rejected_request",
                      "message": "the provider rejected the request as invalid",
                      "detail": result.data, "attempts": attempts},
        }, headers)

    if result.retry_after_s:
        headers["retry-after"] = str(result.retry_after_s)
    return _json(result.http_status, {
        "trace_id": ctx.trace_id,
        "error": {"code": result.status.value, "message": _MESSAGES[result.status],
                  "attempts": attempts},
    }, headers)


def _audit(
    gw: Gateway,
    request: Request,
    ctx: _Ctx,
    status: str,
    http_code: int,
    result: DispatchResult | None = None,
    body: dict[str, Any] | None = None,
) -> None:
    total_ms = round(ctx.elapsed_ms(), 2)
    attempts = result.attempts if result else []
    provider = result.provider if result else None
    request.scope["state"]["log_fields"] = {
        "outcome": status, "provider": provider, "attempts": len(attempts), "tenant": ctx.tenant,
    }

    payload: dict[str, Any] | None = None
    if attempts:
        s = gw.settings
        max_bytes = s.audit_max_body_bytes
        attempt_docs: list[AttemptDoc] = []
        for a in attempts:
            doc: AttemptDoc = {
                "provider": a.provider, "attempt": a.attempt, "outcome": a.outcome.value,
                "status_code": a.status_code, "latency_ms": round(a.latency_ms, 2),
                "error": a.error, "probe": a.is_probe,
            }
            if s.audit_capture_bodies:
                doc["raw_response"] = clip(a.response, a.response_size, max_bytes)
            attempt_docs.append(doc)
        payload = {"attempts": attempt_docs}
        if s.audit_capture_bodies:
            payload["request"] = clip(body, len(ctx.raw_body), max_bytes)

    event: AuditEvent = {
        "v": SCHEMA_VERSION,
        "trace_id": ctx.trace_id,
        "client_request_id": ctx.client_request_id,
        "received_at_us": int(ctx.received_at * 1_000_000),
        "tenant_id": ctx.tenant,
        "idempotency_key": ctx.idem_key,
        "provider": provider,
        "attempt_count": len(attempts),
        "status": status,
        "http_code": http_code,
        "total_ms": total_ms,
        "provider_ms": round(result.provider_ms, 2) if result and result.provider_ms else None,
        "error_class": result.error_class if result else None,
        "payload": payload,
    }
    gw.audit.emit(event, tracing.carrier_for_current())
