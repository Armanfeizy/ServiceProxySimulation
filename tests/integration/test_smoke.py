"""End-to-end smoke test against the running compose stack.

  docker compose up -d && pytest -m integration tests/integration/test_smoke.py
GATEWAY_URL defaults to http://127.0.0.1:8088.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

pytestmark = pytest.mark.integration

URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8088")


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=URL, timeout=20, trust_env=False) as c:
        try:
            await c.get("/healthz")
        except httpx.TransportError:
            pytest.skip(f"no gateway at {URL}")
        yield c


async def test_process_round_trip_and_audit_lookup(client: httpx.AsyncClient) -> None:
    response = await client.post("/v1/process", json={"hello": "world"},
                                 headers={"X-Request-ID": "smoke-1"})
    trace_id = response.headers["x-trace-id"]
    assert response.headers["x-request-id"] == "smoke-1"
    assert response.status_code in (200, 502, 503, 504), response.text
    body = response.json()
    assert body["trace_id"] == trace_id
    if response.status_code == 200:
        assert body["provider"] in {"A", "B", "C"}
        assert body["result"]["echo"] == {"hello": "world"}

    # The audit path is asynchronous (buffer -> Kafka -> consumer): poll for it.
    for _ in range(40):
        lookup = await client.get(f"/v1/requests/{trace_id}")
        if lookup.status_code == 200 and lookup.json()["summary"] and lookup.json()["payload"]:
            break
        await asyncio.sleep(0.5)
    assert lookup.status_code == 200, lookup.text
    record = lookup.json()
    assert record["summary"]["trace_id"] == trace_id
    assert record["summary"]["client_request_id"] == "smoke-1"
    assert record["summary"]["http_code"] == response.status_code
    assert record["payload"]["attempts"]


async def test_idempotency_replay_and_key_reuse(client: httpx.AsyncClient) -> None:
    key = f"smoke-{uuid.uuid4().hex}"
    first = await client.post("/v1/process", json={"n": 1}, headers={"Idempotency-Key": key})
    if first.status_code != 200:
        pytest.skip(f"first call was not final ({first.status_code}); key released by design")
    second = await client.post("/v1/process", json={"n": 1}, headers={"Idempotency-Key": key})
    assert second.status_code == 200
    assert second.headers.get("idempotent-replayed") == "true"
    assert second.json()["trace_id"] == first.json()["trace_id"]    # same answer, zero upstream calls
    reused = await client.post("/v1/process", json={"n": 2}, headers={"Idempotency-Key": key})
    assert reused.status_code == 422


async def test_validation(client: httpx.AsyncClient) -> None:
    assert (await client.post("/v1/process", content=b"[1,2]",
                              headers={"content-type": "application/json"})).status_code == 422
    assert (await client.post("/v1/process", content=b"{not json")).status_code == 422
    big = b'{"x": "' + b"a" * 300_000 + b'"}'
    assert (await client.post("/v1/process", content=big)).status_code == 413


async def test_providers_endpoint(client: httpx.AsyncClient) -> None:
    state = (await client.get("/v1/providers")).json()
    assert set(state["providers"]) == {"A", "B", "C"}
    for provider in state["providers"].values():
        assert provider["breaker"] in {"closed", "half_open", "open"}
