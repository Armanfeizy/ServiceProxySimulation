"""Read side of the audit store: GET /v1/requests/{trace_id}.

Joins the Postgres summary row with the Mongo payload document. Connections are opened
lazily, so a database outage only affects this endpoint, never /v1/process.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import asyncpg
from pymongo import AsyncMongoClient
from ulid import ULID

from gateway.settings import Settings

_SUMMARY_COLUMNS = (
    "received_at, trace_id, client_request_id, tenant_id, idempotency_key, provider, "
    "attempt_count, status, "
    "http_code, total_ms, provider_ms, error_class"
)


class AuditLookup:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pg: asyncpg.Pool | None = None
        self._mongo: AsyncMongoClient[dict[str, Any]] | None = None
        self._lock = asyncio.Lock()

    async def _clients(self) -> tuple[asyncpg.Pool, AsyncMongoClient[dict[str, Any]]]:
        async with self._lock:
            if self._pg is None:
                self._pg = await asyncpg.create_pool(
                    self._settings.postgres_dsn, min_size=0, max_size=4, timeout=3
                )
            if self._mongo is None:
                self._mongo = AsyncMongoClient(
                    self._settings.mongo_url, serverSelectionTimeoutMS=3000, tz_aware=True
                )
        return self._pg, self._mongo

    async def get(self, trace_id: str) -> dict[str, Any] | None:
        try:
            # trace ids are gateway-minted ULIDs: the embedded timestamp prunes
            # partitions, so this is a primary-key lookup in one daily partition.
            received_at = ULID.from_str(trace_id).datetime
        except ValueError:
            return None
        pg, mongo = await self._clients()

        async def summary_row() -> asyncpg.Record | None:
            async with pg.acquire() as conn:
                return await conn.fetchrow(
                    f"SELECT {_SUMMARY_COLUMNS} FROM audit_request "
                    "WHERE trace_id = $1 AND received_at BETWEEN $2 AND $3",
                    trace_id, received_at - timedelta(seconds=1), received_at + timedelta(seconds=1),
                )

        row, payload = await asyncio.gather(
            summary_row(),
            mongo[self._settings.mongo_db]["audit_payloads"].find_one({"_id": trace_id}),
        )
        if row is None and payload is None:
            return None
        summary = dict(row) if row else None
        if summary:
            summary["received_at"] = summary["received_at"].isoformat()
            for column in ("total_ms", "provider_ms"):  # float4 -> no 4082.97998046875
                if summary[column] is not None:
                    summary[column] = round(summary[column], 2)
        if payload:
            payload.pop("_id", None)
            if isinstance(payload.get("received_at"), datetime):
                payload["received_at"] = payload["received_at"].isoformat()
        return {"trace_id": trace_id, "summary": summary, "payload": payload}

    async def aclose(self) -> None:
        if self._pg is not None:
            await self._pg.close()
        if self._mongo is not None:
            await self._mongo.close()
