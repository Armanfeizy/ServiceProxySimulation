"""Audit consumer: Kafka -> Postgres (summary) + Mongo (payload).

At-least-once with idempotent sinks:
  * read up to `consumer_batch_size` records,
  * Postgres: COPY into a session temp table, then INSERT ... ON CONFLICT DO NOTHING
    (COPY speed, and replays of the same record are harmless),
  * Mongo: insert_many(ordered=False) with _id = trace_id (duplicates fail harmlessly),
  * commit offsets only after BOTH sinks hold the batch.

If a sink is down we keep retrying the same batch and stop committing; Kafka retains
the data and we catch up on recovery. Callers of /v1/process never notice.
Undecodable records go to the DLQ topic instead of blocking the partition forever.

Run: python -m gateway.audit.consumer
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from datetime import UTC, datetime
from typing import Any

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.errors import CommitFailedError, KafkaError
from prometheus_client import start_http_server
from pymongo import AsyncMongoClient
from pymongo.errors import BulkWriteError, PyMongoError

from gateway.audit.events import AuditEvent, decode
from gateway.observability import metrics, tracing
from gateway.observability.logging import setup_logging
from gateway.settings import Settings, get_settings

log = structlog.get_logger("gateway.audit.consumer")

COLUMNS = (
    "received_at", "trace_id", "client_request_id", "tenant_id", "idempotency_key", "provider",
    "attempt_count", "status", "http_code", "total_ms", "provider_ms", "error_class",
)
_COLS = ", ".join(COLUMNS)
_STAGE_DDL = (
    "CREATE TEMP TABLE IF NOT EXISTS audit_stage (LIKE audit_request) ON COMMIT DELETE ROWS"
)
_INSERT = f"INSERT INTO audit_request ({_COLS}) SELECT {_COLS} FROM audit_stage ON CONFLICT DO NOTHING"
_DUPLICATE_KEY = 11000


def to_row(event: AuditEvent) -> tuple[Any, ...]:
    return (
        datetime.fromtimestamp(event["received_at_us"] / 1_000_000, UTC),
        event["trace_id"],
        event.get("client_request_id"),
        event.get("tenant_id"),
        event.get("idempotency_key"),
        event.get("provider"),
        event.get("attempt_count"),
        event.get("status"),
        event.get("http_code"),
        event.get("total_ms"),
        event.get("provider_ms"),
        event.get("error_class"),
    )


def to_document(event: AuditEvent) -> dict[str, Any] | None:
    payload = event.get("payload")
    if not payload:
        return None
    return {
        "_id": event["trace_id"],
        "received_at": datetime.fromtimestamp(event["received_at_us"] / 1_000_000, UTC),
        "tenant_id": event.get("tenant_id"),
        "status": event.get("status"),
        "provider": event.get("provider"),
        "http_code": event.get("http_code"),
        "request": payload.get("request"),
        "attempts": payload.get("attempts", []),
    }


class AuditWriter:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pg: asyncpg.Pool | None = None
        self._mongo: AsyncMongoClient[dict[str, Any]] | None = None

    async def connect(self) -> None:
        self._pg = await asyncpg.create_pool(
            self._settings.postgres_dsn, min_size=1, max_size=2,
            init=lambda conn: conn.execute(_STAGE_DDL),
        )
        async with self._pg.acquire() as conn:
            created = await conn.fetchval("SELECT ensure_audit_partitions(3)")
        log.info("postgres_ready", partitions_created=created)
        self._mongo = AsyncMongoClient(self._settings.mongo_url, serverSelectionTimeoutMS=5000)
        await self._mongo.admin.command("ping")
        log.info("mongo_ready")

    async def write(self, events: list[AuditEvent]) -> None:
        await asyncio.gather(self._write_pg(events), self._write_mongo(events))

    async def _write_pg(self, events: list[AuditEvent]) -> None:
        assert self._pg is not None
        rows = [to_row(e) for e in events]
        started = time.perf_counter()
        try:
            async with self._pg.acquire() as conn, conn.transaction():
                await conn.copy_records_to_table("audit_stage", records=rows, columns=COLUMNS)
                await conn.execute(_INSERT)
        except Exception:
            metrics.CONSUMER_WRITE_ERRORS.labels("postgres").inc()
            raise
        metrics.CONSUMER_BATCH_SECONDS.labels("postgres").observe(time.perf_counter() - started)
        metrics.CONSUMER_ROWS.labels("postgres").inc(len(rows))

    async def _write_mongo(self, events: list[AuditEvent]) -> None:
        assert self._mongo is not None
        docs = [d for d in (to_document(e) for e in events) if d is not None]
        if not docs:
            return
        started = time.perf_counter()
        collection = self._mongo[self._settings.mongo_db]["audit_payloads"]
        try:
            await collection.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            # Duplicates are expected under at-least-once; anything else is real.
            real = [e for e in exc.details.get("writeErrors", []) if e.get("code") != _DUPLICATE_KEY]
            if real or exc.details.get("writeConcernErrors"):
                metrics.CONSUMER_WRITE_ERRORS.labels("mongo").inc()
                raise
        except PyMongoError:
            metrics.CONSUMER_WRITE_ERRORS.labels("mongo").inc()
            raise
        metrics.CONSUMER_BATCH_SECONDS.labels("mongo").observe(time.perf_counter() - started)
        metrics.CONSUMER_ROWS.labels("mongo").inc(len(docs))

    async def close(self) -> None:
        if self._pg is not None:
            await self._pg.close()
        if self._mongo is not None:
            await self._mongo.close()


async def _retry_forever(what: str, fn: Any, stop: asyncio.Event) -> bool:
    backoff = 0.5
    while not stop.is_set():
        try:
            await fn()
            return True
        except Exception as exc:
            log.warning("retrying", what=what, error=repr(exc)[:300], retry_in_s=backoff)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, 10.0)
    return False


class AuditConsumer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._writer = AuditWriter(settings)
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq: AIOKafkaProducer | None = None
        self.stop = asyncio.Event()

    async def _start_kafka(self) -> None:
        s = self._settings
        consumer = AIOKafkaConsumer(
            s.audit_topic,
            bootstrap_servers=s.kafka_bootstrap,
            group_id=s.consumer_group,
            client_id="audit-consumer",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=s.consumer_batch_size,
            fetch_max_bytes=32 * 1024 * 1024,
            max_poll_interval_ms=600_000,
        )
        dlq = AIOKafkaProducer(bootstrap_servers=s.kafka_bootstrap, client_id="audit-dlq", acks="all")
        try:
            await consumer.start()
            await dlq.start()
        except Exception:
            with contextlib.suppress(Exception):
                await consumer.stop()
            with contextlib.suppress(Exception):
                await dlq.stop()
            raise
        self._consumer, self._dlq = consumer, dlq

    async def run(self) -> None:
        if not await _retry_forever("sinks_connect", self._writer.connect, self.stop):
            return
        if not await _retry_forever("kafka_connect", self._start_kafka, self.stop):
            return
        assert self._consumer is not None
        log.info("audit_consumer_started", topic=self._settings.audit_topic)
        try:
            while not self.stop.is_set():
                batches = await self._consumer.getmany(
                    timeout_ms=500, max_records=self._settings.consumer_batch_size
                )
                records = [r for part in batches.values() for r in part]
                if records:
                    await self._handle(records)
                await self._export_lag()
        finally:
            await self._shutdown()

    async def _handle(self, records: list[ConsumerRecord[bytes, bytes]]) -> None:
        assert self._consumer is not None
        events: list[AuditEvent] = []
        links = []
        for record in records:
            try:
                events.append(decode(record.value or b""))
            except ValueError as exc:
                await self._to_dlq(record, repr(exc))
                continue
            link = tracing.extract_link({k: v.decode() for k, v in record.headers or ()})
            if link is not None:
                links.append(link)

        if events:
            # One span per batch, linked to every sampled request trace it persists.
            with tracing.tracer().start_as_current_span(
                "audit persist batch", links=links[:128],
                attributes={"batch.size": len(events)},
            ):
                ok = await _retry_forever("persist_batch", lambda: self._writer.write(events),
                                          self.stop)
            if not ok:
                return  # shutting down: do NOT commit; the batch is redelivered
            metrics.CONSUMER_BATCH_ROWS.observe(len(events))

        try:
            await self._consumer.commit()
        except CommitFailedError as exc:
            # Rebalanced away mid-batch: the new owner re-reads, sinks dedupe.
            log.warning("commit_failed_after_rebalance", error=repr(exc))

    async def _to_dlq(self, record: ConsumerRecord[bytes, bytes], reason: str) -> None:
        dlq = self._dlq
        assert dlq is not None
        metrics.CONSUMER_DLQ.inc()
        log.error("poison_record_to_dlq", partition=record.partition, offset=record.offset,
                  reason=reason)
        headers = [*(record.headers or ()), ("dlq.reason", reason.encode()[:500]),
                   ("dlq.source.offset", str(record.offset).encode())]
        await _retry_forever(
            "dlq_send",
            lambda: dlq.send_and_wait(
                self._settings.audit_dlq_topic, value=record.value, key=record.key,
                headers=headers,
            ),
            self.stop,
        )

    async def _export_lag(self) -> None:
        assert self._consumer is not None
        for tp in self._consumer.assignment():
            highwater = self._consumer.highwater(tp)
            if highwater is None:
                continue
            with contextlib.suppress(KafkaError):
                position = await self._consumer.position(tp)
                metrics.CONSUMER_LAG.labels(str(tp.partition)).set(max(0, highwater - position))

    async def _shutdown(self) -> None:
        for client in (self._consumer, self._dlq):
            if client is not None:
                with contextlib.suppress(KafkaError, OSError):
                    await client.stop()
        await self._writer.close()
        log.info("audit_consumer_stopped")


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, "audit-consumer")
    tracing.setup_tracing(
        enabled=settings.otel_enabled, service="audit-consumer",
        endpoint=settings.otel_endpoint, sample_ratio=settings.otel_sample_ratio,
    )
    start_http_server(9100)
    consumer = AuditConsumer(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, consumer.stop.set)
    try:
        await consumer.run()
    finally:
        tracing.shutdown_tracing()


if __name__ == "__main__":
    asyncio.run(main())
