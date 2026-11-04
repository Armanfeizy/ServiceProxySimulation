"""Audit emission: a bounded in-memory buffer drained to Kafka off the request path.

`emit()` is O(1) and never blocks. If the buffer is full (Kafka slow or down) the event
is dropped and counted: we never make a caller wait to write a log line.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from gateway.audit.events import AuditEvent, encode
from gateway.observability import metrics
from gateway.settings import Settings

log = structlog.get_logger(__name__)

_DRAIN_MAX = 1_000
_YIELD_EVERY = 64   # encode+append is CPU work: let request handlers run in between
_Item = tuple[AuditEvent, dict[str, str]]


class AuditProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queue: asyncio.Queue[_Item] = asyncio.Queue(
            maxsize=settings.audit_buffer_size
        )
        self._producer: AIOKafkaProducer | None = None
        self._task: asyncio.Task[None] | None = None
        self.connected = False

    # ------------------------------------------------------------ request path

    def emit(self, event: AuditEvent, trace_carrier: dict[str, str]) -> None:
        if not self._settings.audit_enabled:
            return
        try:
            self._queue.put_nowait((event, trace_carrier))
        except asyncio.QueueFull:
            metrics.AUDIT_DROPPED.labels("buffer_full").inc()

    def depth(self) -> int:
        return self._queue.qsize()

    # ------------------------------------------------------------ background

    async def start(self) -> None:
        if self._settings.audit_enabled:
            self._task = asyncio.create_task(self._run(), name="audit-producer")

    async def stop(self, flush_timeout_s: float = 5.0) -> None:
        if self._task is None:
            return
        # Give the drain loop a moment to empty the buffer, then stop.
        if self.connected:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(flush_timeout_s):
                    await self._queue.join()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        if self._producer is not None:
            with contextlib.suppress(KafkaError, OSError):
                await self._producer.stop()

    async def _connect(self) -> AIOKafkaProducer:
        backoff = 0.5
        while True:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap,
                client_id=f"{self._settings.service_name}-audit",
                acks="all",
                linger_ms=self._settings.kafka_linger_ms,
                compression_type="lz4",
                max_batch_size=512 * 1024,
                request_timeout_ms=10_000,
            )
            try:
                await producer.start()
            except (KafkaError, OSError) as exc:
                with contextlib.suppress(Exception):
                    await producer.stop()
                log.warning("kafka_connect_failed", error=repr(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            log.info("kafka_producer_connected", bootstrap=self._settings.kafka_bootstrap)
            return producer

    async def _run(self) -> None:
        producer = self._producer = await self._connect()
        self.connected = True
        topic = self._settings.audit_topic
        backoff = 0.5
        while True:
            first = await self._queue.get()
            batch = [first]
            while len(batch) < _DRAIN_MAX:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            metrics.AUDIT_BUFFER_DEPTH.set(self._queue.qsize())

            sent = 0
            try:
                for n, item in enumerate(batch):
                    if n and n % _YIELD_EVERY == 0:
                        await asyncio.sleep(0)
                    event, carrier = item
                    future = await producer.send(
                        topic,
                        value=encode(event),
                        key=event["trace_id"].encode(),
                        headers=[(k, v.encode()) for k, v in carrier.items()],
                    )
                    future.add_done_callback(functools.partial(self._on_delivery, item))
                    sent += 1
            except (KafkaError, OSError) as exc:
                # Accumulator full / no metadata: keep the unsent tail for later.
                # aiokafka reconnects on its own; we just back off.
                metrics.KAFKA_PRODUCE_ERRORS.inc()
                self._mark_disconnected(exc)
                for item in batch[sent:]:
                    self._requeue(item)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
            else:
                backoff = 0.5
            metrics.AUDIT_PRODUCED.inc(sent)
            for _ in batch:
                self._queue.task_done()

    def _on_delivery(self, item: _Item, future: asyncio.Future[object]) -> None:
        if future.cancelled():
            return
        exc = future.exception()
        if exc is None:
            if not self.connected:
                self.connected = True
                log.warning("kafka_delivery_recovered")
            return
        # Delivery failed after aiokafka's own retries (e.g. broker down longer than
        # request_timeout_ms). Put it back: it is retried until the buffer is full.
        metrics.KAFKA_PRODUCE_ERRORS.inc()
        self._mark_disconnected(exc)
        self._requeue(item)

    def _requeue(self, item: _Item) -> None:
        try:
            self._queue.put_nowait(item)
            metrics.AUDIT_REQUEUED.inc()
        except asyncio.QueueFull:
            metrics.AUDIT_DROPPED.labels("buffer_full").inc()

    def _mark_disconnected(self, exc: BaseException) -> None:
        if self.connected:
            self.connected = False
            log.error("kafka_delivery_failing_buffering", error=repr(exc),
                      buffer_capacity=self._settings.audit_buffer_size)
