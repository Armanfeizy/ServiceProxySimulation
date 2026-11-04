"""Request context as a pure ASGI middleware (BaseHTTPMiddleware costs too much per request).

Every request gets a gateway-minted ULID `trace_id`: unique (the audit store dedupes on
it), time-sortable, and it embeds its own timestamp (lookups prune partitions with it).
A caller-supplied `X-Request-ID` is kept as `client_request_id` — logged, audited and
echoed back — and a W3C `traceparent` is continued by the server span. The trace_id is
bound to every log line, returned as `X-Trace-ID`, and forwarded to providers.
"""

from __future__ import annotations

import re
import time

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from ulid import ULID

from gateway.observability import metrics, tracing

log = structlog.get_logger("gateway.access")

_VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")
_ROUTES = ("/v1/process", "/v1/providers", "/v1/requests", "/healthz", "/readyz", "/metrics")
_QUIET = frozenset({"/metrics", "/healthz", "/readyz"})


def client_request_id(supplied: str | None) -> str | None:
    return supplied if supplied and _VALID_REQUEST_ID.match(supplied) else None


def route_label(path: str) -> str:
    # Bounded label cardinality: never use raw paths (ids!) as metric labels.
    for prefix in _ROUTES:
        if path == prefix or path.startswith(prefix + "/"):
            return prefix
    return "other"


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = time.monotonic()
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
        ulid = ULID()
        trace_id = str(ulid)
        client_id = client_request_id(headers.get("x-request-id"))
        structlog.contextvars.clear_contextvars()
        if client_id:
            structlog.contextvars.bind_contextvars(trace_id=trace_id, client_request_id=client_id)
        else:
            structlog.contextvars.bind_contextvars(trace_id=trace_id)

        state = scope.setdefault("state", {})
        state["trace_id"] = trace_id
        state["client_request_id"] = client_id
        state["received_at"] = time.time()
        state["started"] = started

        status = 500
        extra_headers = [
            (b"x-trace-id", trace_id.encode()),
            (b"x-request-id", (client_id or trace_id).encode()),
        ]

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [*message.get("headers", []), *extra_headers]
            await send(message)

        path: str = scope["path"]
        route = route_label(path)
        with tracing.server_span(f"{scope['method']} {route}", headers, int(ulid)) as span:
            span.set_attribute("gateway.trace_id", trace_id)
            try:
                await self.app(scope, receive, send_wrapper)
            finally:
                elapsed = time.monotonic() - started
                span.set_attribute("http.response.status_code", status)
                metrics.HTTP_REQUEST_DURATION.labels(route, str(status)).observe(elapsed)
                if path not in _QUIET:
                    log.info(
                        "request",
                        method=scope["method"],
                        route=route,
                        status=status,
                        duration_ms=round(elapsed * 1000, 1),
                        **state.get("log_fields", {}),
                    )
