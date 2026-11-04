"""Upstream HTTP transport.

The dispatcher talks to this narrow interface, never to an HTTP library directly. The
transport owns the connection pool and turns library exceptions into outcomes that say
what the failure *proves* (see resilience/outcomes.py).

aiohttp, not httpx: under load a slow provider pins thousands of open sockets per pool
(Little's Law), and httpcore's pool does an O(n) scan of every connection on each
request and each release. Profiling at 400 RPS put 62% of the CPU in
`AsyncConnectionPool._assign_requests_to_connections`. aiohttp acquires in O(1).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

import aiohttp

from gateway.resilience.outcomes import Outcome


@dataclass(frozen=True, slots=True)
class RawResponse:
    status: int
    headers: Mapping[str, str]
    content: bytes


class TransportError(Exception):
    """An upstream call that produced no usable HTTP response."""

    def __init__(self, outcome: Outcome, detail: str) -> None:
        super().__init__(detail)
        self.outcome = outcome
        self.detail = detail


class Transport(Protocol):
    async def post(
        self,
        url: str,
        content: bytes,
        headers: Mapping[str, str],
        *,
        timeout_s: float,
        connect_timeout_s: float,
    ) -> RawResponse: ...

    async def aclose(self) -> None: ...


def classify_aiohttp_error(exc: BaseException) -> Outcome:
    # Order matters: the timeout classes subclass both ClientError and TimeoutError.
    if isinstance(exc, aiohttp.ConnectionTimeoutError | aiohttp.ClientConnectorError):
        return Outcome.CONNECT_ERROR          # never arrived: safe to send elsewhere
    if isinstance(exc, aiohttp.SocketTimeoutError | TimeoutError):
        return Outcome.TIMEOUT                # sent, then we stopped waiting: unknown
    return Outcome.PROTOCOL_ERROR             # reset / broken response: unknown


class AiohttpTransport:
    """One connection pool per provider (bounded = the provider's bulkhead)."""

    def __init__(self, max_connections: int, proxy: str | None = None) -> None:
        self._proxy = proxy
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=max_connections,
                limit_per_host=0,
                ttl_dns_cache=60,
                keepalive_timeout=30,
            ),
            # Upstream routing is explicit config; never inherit HTTP(S)_PROXY.
            trust_env=False,
            auto_decompress=True,
            raise_for_status=False,
        )

    async def post(
        self,
        url: str,
        content: bytes,
        headers: Mapping[str, str],
        *,
        timeout_s: float,
        connect_timeout_s: float,
    ) -> RawResponse:
        # `connect` also bounds waiting for a pooled connection, which the bulkhead
        # already makes rare; the total budget is enforced with asyncio.timeout so it
        # covers every phase uniformly.
        timeout = aiohttp.ClientTimeout(
            total=None, connect=connect_timeout_s, sock_connect=connect_timeout_s
        )
        try:
            async with asyncio.timeout(timeout_s):
                async with self._session.post(
                    url, data=content, headers=dict(headers), timeout=timeout, proxy=self._proxy
                ) as response:
                    body = await response.read()
                    return RawResponse(response.status, response.headers, body)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise TransportError(classify_aiohttp_error(exc), f"{type(exc).__name__}: {exc}"[:300]) from exc

    async def aclose(self) -> None:
        await self._session.close()
