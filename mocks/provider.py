"""Local mock of the starter-kit providers (POST /v1/process).

The real hosts are not reachable from every network, and benchmarking 10k RPS across
the public internet measures *their* network, not the gateway. These mocks reproduce
the documented profiles and are configured purely by environment:

  PROVIDER_NAME        A | B | C
  LATENCY_MIN_MS       lower bound of the uniform latency band
  LATENCY_MAX_MS       upper bound
  ERROR_RATE           fraction answered with a 5xx from ERROR_CODES
  ERROR_CODES          e.g. "500,502,503"
  TIMEOUT_RATE         fraction that hang for HANG_MS (forces caller timeouts)
  HANG_MS              how long a "timeout" request hangs
  RATE_LIMIT_RPS       0 = unlimited; otherwise 429 + Retry-After above this rate
  RATE_LIMIT_BURST     bucket size

POST /admin/config with a JSON object of the same (lower-case) keys changes the profile
at runtime — used to demo the circuit breaker and AIMD. GET /admin/config shows it.
Raw ASGI on purpose: the mock must not be the bottleneck in the overhead benchmark.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
from typing import Any

Scope = dict[str, Any]


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


CONFIG: dict[str, Any] = {
    "provider_name": os.environ.get("PROVIDER_NAME", "X"),
    "latency_min_ms": _env_float("LATENCY_MIN_MS", 300),
    "latency_max_ms": _env_float("LATENCY_MAX_MS", 1000),
    "error_rate": _env_float("ERROR_RATE", 0.0),
    "error_codes": [int(c) for c in os.environ.get("ERROR_CODES", "500,502,503").split(",") if c],
    "timeout_rate": _env_float("TIMEOUT_RATE", 0.0),
    "hang_ms": _env_float("HANG_MS", 30_000),
    "rate_limit_rps": _env_float("RATE_LIMIT_RPS", 0),
    "rate_limit_burst": _env_float("RATE_LIMIT_BURST", 0),
}

STATS = {"requests": 0, "ok": 0, "rate_limited": 0, "errors": 0, "hung": 0}


class Bucket:
    def __init__(self) -> None:
        self.tokens = 0.0
        self.last = time.monotonic()
        self.reset()

    def reset(self) -> None:
        self.tokens = CONFIG["rate_limit_burst"] or CONFIG["rate_limit_rps"]
        self.last = time.monotonic()

    def take(self) -> float:
        """0 if allowed, else seconds until a token is available."""
        rate = CONFIG["rate_limit_rps"]
        if rate <= 0:
            return 0.0
        burst = CONFIG["rate_limit_burst"] or rate
        now = time.monotonic()
        self.tokens = min(burst, self.tokens + (now - self.last) * rate)
        self.last = now
        if self.tokens >= 1:
            self.tokens -= 1
            return 0.0
        return (1 - self.tokens) / rate


BUCKET = Bucket()


async def _read_body(receive: Any) -> bytes:
    chunks = []
    while True:
        message = await receive()
        chunks.append(message.get("body", b""))
        if not message.get("more_body"):
            return b"".join(chunks)


async def _send(
    send: Any, status: int, body: dict[str, Any], headers: list[tuple[bytes, bytes]] | None = None
) -> None:
    payload = json.dumps(body).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [(b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode()), *(headers or [])],
    })
    await send({"type": "http.response.body", "body": payload})


async def _process(scope: Scope, receive: Any, send: Any) -> None:
    raw = await _read_body(receive)
    STATS["requests"] += 1
    headers = {k.decode(): v.decode() for k, v in scope["headers"]}
    name = CONFIG["provider_name"]

    wait = BUCKET.take()
    if wait > 0:
        STATS["rate_limited"] += 1
        await _send(send, 429, {"error": "rate limit exceeded", "provider": name},
                    [(b"retry-after", str(max(1, math.ceil(wait))).encode())])
        return

    roll = random.random()
    if roll < CONFIG["timeout_rate"]:
        STATS["hung"] += 1
        await asyncio.sleep(CONFIG["hang_ms"] / 1000)
        await _send(send, 504, {"error": "upstream timeout", "provider": name})
        return

    lo, hi = CONFIG["latency_min_ms"], CONFIG["latency_max_ms"]
    await asyncio.sleep(random.uniform(lo, max(lo, hi)) / 1000)

    if roll < CONFIG["timeout_rate"] + CONFIG["error_rate"]:
        STATS["errors"] += 1
        code = random.choice(CONFIG["error_codes"] or [500])
        await _send(send, code, {"error": "internal provider error", "provider": name})
        return

    try:
        body = json.loads(raw) if raw else {}
    except ValueError:
        await _send(send, 400, {"error": "invalid json", "provider": name})
        return
    STATS["ok"] += 1
    await _send(send, 200, {
        "provider": name,
        "status": "processed",
        "request_id": headers.get("x-request-id"),
        "processed_at": time.time(),
        "echo": body,
    })


async def _admin(scope: Scope, receive: Any, send: Any) -> None:
    if scope["method"] == "POST":
        update = json.loads(await _read_body(receive) or b"{}")
        for key, value in update.items():
            if key in CONFIG and key != "provider_name":
                CONFIG[key] = value
        BUCKET.reset()
    await _send(send, 200, {"config": CONFIG, "stats": STATS})


async def app(scope: Scope, receive: Any, send: Any) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    path, method = scope["path"], scope["method"]
    if path == "/v1/process" and method == "POST":
        await _process(scope, receive, send)
    elif path == "/admin/config":
        await _admin(scope, receive, send)
    elif path == "/healthz":
        await _send(send, 200, {"status": "ok"})
    else:
        await _send(send, 404, {"error": "not found"})
