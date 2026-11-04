"""The Redis Lua scripts must implement the same policy as the Python reference.

Needs a Redis: TEST_REDIS_URL (default redis://127.0.0.1:6390/15), e.g.
  docker run --rm -d -p 6390:6379 redis:7-alpine
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from gateway.resilience.scripts import LuaScript, ScriptPipeline

pytestmark = pytest.mark.integration

URL = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6390/15")


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    # Same pool type as production: concurrent callers wait for a connection.
    client = Redis(connection_pool=BlockingConnectionPool.from_url(URL, max_connections=32))
    try:
        await client.ping()
    except (RedisConnectionError, OSError):
        pytest.skip(f"no redis at {URL}")
    yield client
    await client.aclose()


def key() -> str:
    return f"test:{uuid.uuid4().hex}"


async def test_token_bucket_burst_and_retry_after(redis: Redis) -> None:
    script = LuaScript(redis, "token_bucket.lua")
    k = key()
    results = [await script([k], [10, 5, 1]) for _ in range(6)]
    assert [r[0] for r in results] == [1, 1, 1, 1, 1, 0]
    assert 0 < results[-1][1] <= 100
    await asyncio.sleep(0.25)
    assert (await script([k], [10, 5, 1]))[0] == 1


async def test_token_bucket_is_atomic_under_concurrency(redis: Redis) -> None:
    script = LuaScript(redis, "token_bucket.lua")
    k = key()
    results = await asyncio.gather(*(script([k], [0.001, 50, 1]) for _ in range(200)))
    assert sum(r[0] for r in results) == 50   # never over-granted across "replicas"


async def test_aimd_one_decrease_per_window(redis: Redis) -> None:
    script = LuaScript(redis, "aimd.lua")
    k = key()
    args = [200, 10, 0.5, 10, 1000]
    first = await script([k], ["decrease", *args, 2000])
    again = await script([k], ["decrease", *args, 0])
    assert float(first[0]) == 100.0 and float(again[0]) == 100.0
    assert 1500 < int(first[1]) <= 2000             # Retry-After became a block window
    tick = await script([k], ["tick", *args, 0])
    assert float(tick[0]) == 100.0                  # window not clean yet
    await asyncio.sleep(1.05)
    tick = await script([k], ["tick", *args, 0])
    assert float(tick[0]) == 110.0


async def test_breaker_trips_cools_and_probes(redis: Redis) -> None:
    script = LuaScript(redis, "breaker.lua")
    state, counts, probe = key(), key(), key()
    now_s = int((await redis.time())[0])
    await redis.hset(counts, mapping={f"{now_s}:t": 25, f"{now_s}:f": 20,
                                      f"{now_s - 60}:t": 999})  # stale bucket, must be ignored
    args = [10, 0.5, 20, 300]
    result = await script([state, counts, probe], ["tick", *args])
    assert result[0] == b"open" and int(result[1]) == 25 and int(result[2]) == 20
    assert not await redis.hexists(counts, f"{now_s - 60}:t")
    await asyncio.sleep(0.35)
    result = await script([state, counts, probe], ["tick", *args])
    assert result[0] == b"half_open"
    assert await redis.set(probe, "r1", nx=True, px=5000)
    assert not await redis.set(probe, "r2", nx=True, px=5000)   # exactly one prober
    result = await script([state, counts, probe], ["probe_ok", *args])
    assert result[0] == b"closed"
    assert not await redis.exists(probe) and not await redis.exists(counts)


async def test_breaker_min_volume(redis: Redis) -> None:
    script = LuaScript(redis, "breaker.lua")
    state, counts, probe = key(), key(), key()
    now_s = int((await redis.time())[0])
    await redis.hset(counts, mapping={f"{now_s}:t": 19, f"{now_s}:f": 19})
    result = await script([state, counts, probe], ["tick", 10, 0.5, 20, 5000])
    assert result[0] == b"closed"


async def test_pipeline_recovers_from_noscript_without_replaying_commands(redis: Redis) -> None:
    """After a Redis restart (script cache empty) only the script is re-run: the
    pipeline's other commands must not be applied twice."""
    script = LuaScript(redis, "token_bucket.lua")
    await redis.script_flush()
    counter, bucket = key(), key()
    sp = ScriptPipeline(redis)
    sp.pipe.incr(counter)
    sp.script(script, [bucket], [10, 5, 1])
    sp.pipe.incr(counter)
    results = await sp.execute()
    assert results[1][0] == 1                  # script ran after being loaded
    assert int(await redis.get(counter)) == 2  # INCRs applied exactly once each
