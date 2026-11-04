"""Audit event shape shared by the producer (api) and the consumer (persistence).

One Kafka message per caller request. `summary` becomes one narrow Postgres row;
`payload` (variable shape, KBs) becomes one Mongo document keyed by trace_id.
"""

from __future__ import annotations

from typing import Any, TypedDict, cast

import orjson

SCHEMA_VERSION = 1


class AttemptDoc(TypedDict, total=False):
    provider: str
    attempt: int
    outcome: str
    status_code: int | None
    latency_ms: float
    error: str | None
    probe: bool
    raw_response: Any


class AuditEvent(TypedDict, total=False):
    v: int
    trace_id: str
    received_at_us: int        # epoch microseconds, UTC
    client_request_id: str | None
    tenant_id: str
    idempotency_key: str | None
    provider: str | None
    attempt_count: int
    status: str                # ok | client_error | provider_error | rate_limited |
                               # unavailable | deadline_exceeded | shed | throttled | ...
    http_code: int
    total_ms: float
    provider_ms: float | None
    error_class: str | None
    payload: dict[str, Any] | None


def clip(value: Any, size_hint: int, max_bytes: int) -> Any:
    """Keep payloads bounded: oversized bodies become a preview, not a multi-MB row."""
    if size_hint <= max_bytes:
        return value
    preview = orjson.dumps(value)[: max_bytes // 4].decode("utf-8", "ignore")
    return {"_truncated": True, "size_bytes": size_hint, "preview": preview}


def encode(event: AuditEvent) -> bytes:
    return orjson.dumps(event)


def decode(raw: bytes) -> AuditEvent:
    event = orjson.loads(raw)
    if not isinstance(event, dict) or "trace_id" not in event or "received_at_us" not in event:
        raise ValueError("not an audit event")
    return cast(AuditEvent, event)
