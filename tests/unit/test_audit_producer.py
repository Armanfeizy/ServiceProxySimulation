"""The audit buffer must survive a Kafka outage longer than aiokafka's own retries."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest

from gateway.audit import producer as producer_module
from gateway.audit.events import AuditEvent
from gateway.audit.producer import AuditProducer
from gateway.observability import metrics
from gateway.settings import Settings


class FlakyKafka:
    """Fails the first `failures` deliveries asynchronously, like an expired batch."""

    failures = 0
    delivered: ClassVar[list[bytes]] = []

    def __init__(self, **_: Any) -> None:
        pass

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, topic: str, *, value: bytes, key: bytes, headers: Any) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if FlakyKafka.failures > 0:
            FlakyKafka.failures -= 1
            future.set_exception(producer_module.KafkaError("batch expired"))
        else:
            FlakyKafka.delivered.append(key)
            future.set_result(None)
        return future


def event(i: int) -> AuditEvent:
    return {"v": 1, "trace_id": f"t{i}", "received_at_us": i, "status": "ok", "http_code": 200}


async def test_failed_deliveries_are_requeued_not_lost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(producer_module, "AIOKafkaProducer", FlakyKafka)
    FlakyKafka.failures, FlakyKafka.delivered = 30, []
    dropped_before = metrics.AUDIT_DROPPED.labels("buffer_full")._value.get()

    audit = AuditProducer(Settings(_env_file=None, audit_buffer_size=1000))  # type: ignore[call-arg]
    await audit.start()
    for i in range(50):
        audit.emit(event(i), {})
    for _ in range(200):
        if len(set(FlakyKafka.delivered)) == 50:
            break
        await asyncio.sleep(0.01)
    await audit.stop(flush_timeout_s=0.5)

    assert set(FlakyKafka.delivered) == {f"t{i}".encode() for i in range(50)}
    assert metrics.AUDIT_DROPPED.labels("buffer_full")._value.get() == dropped_before
    assert audit.connected  # recovered after the failures


async def test_full_buffer_drops_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    audit = AuditProducer(Settings(_env_file=None, audit_buffer_size=3))  # type: ignore[call-arg]
    before = metrics.AUDIT_DROPPED.labels("buffer_full")._value.get()
    for i in range(5):                      # not started: nothing drains
        audit.emit(event(i), {})
    assert audit.depth() == 3
    assert metrics.AUDIT_DROPPED.labels("buffer_full")._value.get() == before + 2
