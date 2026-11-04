"""AiohttpTransport against real local sockets: exception -> outcome must be exact,
because the outcome decides whether a retry could duplicate a processed request."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator

import pytest
from aiohttp import web

from gateway.providers.adapters import GenericJsonAdapter
from gateway.providers.transport import AiohttpTransport, RawResponse, TransportError
from gateway.resilience.outcomes import Outcome
from tests.conftest import provider_config


@pytest.fixture
async def base_url() -> AsyncIterator[str]:
    async def ok(request: web.Request) -> web.Response:
        return web.json_response({"echo": await request.json()}, status=200)

    async def limited(request: web.Request) -> web.Response:
        return web.json_response({"error": "slow down"}, status=429, headers={"Retry-After": "2"})

    async def slow(request: web.Request) -> web.Response:
        await asyncio.sleep(2)
        return web.json_response({})

    app = web.Application()
    app.router.add_post("/ok", ok)
    app.router.add_post("/limited", limited)
    app.router.add_post("/slow", slow)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    yield f"http://127.0.0.1:{port}"
    await runner.cleanup()


@pytest.fixture
async def transport() -> AsyncIterator[AiohttpTransport]:
    t = AiohttpTransport(max_connections=10)
    yield t
    await t.aclose()


async def post(t: AiohttpTransport, url: str, timeout_s: float = 2.0) -> RawResponse:
    return await t.post(url, b'{"a": 1}', {"content-type": "application/json"},
                        timeout_s=timeout_s, connect_timeout_s=0.5)


async def test_success(transport: AiohttpTransport, base_url: str) -> None:
    response = await post(transport, f"{base_url}/ok")
    assert response.status == 200 and b'"echo"' in response.content


async def test_retry_after_header_is_case_insensitive(transport: AiohttpTransport, base_url: str) -> None:
    response = await post(transport, f"{base_url}/limited")
    result = GenericJsonAdapter(provider_config("A")).parse_response(
        response.status, response.headers, response.content
    )
    assert result.outcome is Outcome.RATE_LIMITED and result.retry_after_ms == 2000


async def test_refused_connection_is_connect_error(transport: AiohttpTransport) -> None:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]          # closed again before we connect
    with pytest.raises(TransportError) as err:
        await post(transport, f"http://127.0.0.1:{port}/x")
    assert err.value.outcome is Outcome.CONNECT_ERROR


async def test_slow_response_is_timeout_not_connect_error(transport: AiohttpTransport, base_url: str) -> None:
    with pytest.raises(TransportError) as err:
        await post(transport, f"{base_url}/slow", timeout_s=0.2)
    assert err.value.outcome is Outcome.TIMEOUT    # sent: it may be processing


async def test_dropped_connection_is_protocol_error(transport: AiohttpTransport) -> None:
    async def drop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")         # request arrived ...
        writer.close()                              # ... and the server vanished

    server = await asyncio.start_server(drop, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(TransportError) as err:
            await post(transport, f"http://127.0.0.1:{port}/x")
        assert err.value.outcome is Outcome.PROTOCOL_ERROR
    finally:
        server.close()
        await server.wait_closed()
