"""Lua scripts without redis-py's per-pipeline SCRIPT EXISTS round-trip.

redis-py's Script objects, when queued on a pipeline, make every `execute()` first send
`SCRIPT EXISTS` (one extra round-trip per request on the hot path). Here we queue plain
EVALSHA and only load the script when Redis answers NOSCRIPT (first use, or after a
Redis restart) — and then re-run *only* that command, never the whole pipeline, whose
other commands (SET NX, HINCRBY) have already been applied.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from importlib import resources
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import NoScriptError

Arg = str | int | float | bytes


def load_lua(name: str) -> str:
    return resources.files("gateway.resilience").joinpath("lua", name).read_text()


class LuaScript:
    def __init__(self, redis: Redis, name: str) -> None:
        self._redis = redis
        self.name = name
        self.source = load_lua(name)
        self.sha = hashlib.sha1(self.source.encode()).hexdigest()

    async def load(self) -> None:
        await self._redis.script_load(self.source)

    async def __call__(self, keys: Sequence[str], args: Sequence[Arg]) -> Any:
        try:
            return await self._redis.evalsha(self.sha, len(keys), *keys, *args)
        except NoScriptError:
            await self.load()
            return await self._redis.evalsha(self.sha, len(keys), *keys, *args)


class ScriptPipeline:
    """A non-transactional pipeline that can carry Lua scripts.

    Use `.pipe` for ordinary commands and `.script()` for scripts, in order.
    """

    def __init__(self, redis: Redis) -> None:
        self.pipe: Pipeline = redis.pipeline(transaction=False)
        self._scripts: dict[int, tuple[LuaScript, Sequence[str], Sequence[Arg]]] = {}

    def script(self, script: LuaScript, keys: Sequence[str], args: Sequence[Arg]) -> None:
        self._scripts[len(self.pipe.command_stack)] = (script, keys, args)
        self.pipe.evalsha(script.sha, len(keys), *keys, *args)

    async def execute(self) -> list[Any]:
        results: list[Any] = await self.pipe.execute(raise_on_error=False)
        for index, (script, keys, args) in self._scripts.items():
            if isinstance(results[index], NoScriptError):
                results[index] = await script(keys, args)
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results
