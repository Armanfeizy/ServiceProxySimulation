"""Scheduled maintenance jobs."""

from __future__ import annotations

import asyncio
from typing import Any

import psycopg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from gateway.audit.events import decode
from gateway.jobs.celery_app import app
from gateway.settings import get_settings

log = structlog.get_logger("gateway.jobs")

# Recompute the last few minutes every run: rows can land late (consumer lag),
# and the upsert makes each run idempotent.
ROLLUP_SQL = """
INSERT INTO audit_rollup_1m (bucket, provider, status, requests, p50_ms, p95_ms, p99_ms, avg_ms)
SELECT date_trunc('minute', received_at),
       coalesce(provider, '-'),
       status,
       count(*),
       percentile_cont(0.50) WITHIN GROUP (ORDER BY total_ms),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms),
       percentile_cont(0.99) WITHIN GROUP (ORDER BY total_ms),
       avg(total_ms)
FROM audit_request
WHERE received_at >= date_trunc('minute', now()) - make_interval(mins => %(lookback)s)
  AND received_at <  date_trunc('minute', now())
GROUP BY 1, 2, 3
ON CONFLICT (bucket, provider, status) DO UPDATE
   SET requests = EXCLUDED.requests, p50_ms = EXCLUDED.p50_ms, p95_ms = EXCLUDED.p95_ms,
       p99_ms = EXCLUDED.p99_ms, avg_ms = EXCLUDED.avg_ms
"""


def _pg() -> psycopg.Connection[Any]:
    return psycopg.connect(get_settings().postgres_dsn, autocommit=True, connect_timeout=5)


@app.task(name="gateway.jobs.tasks.ensure_partitions")
def ensure_partitions(days_ahead: int = 3) -> int:
    with _pg() as conn:
        row = conn.execute("SELECT ensure_audit_partitions(%s)", (days_ahead,)).fetchone()
    created = int(row[0]) if row else 0
    log.info("partitions_ensured", created=created, days_ahead=days_ahead)
    return created


@app.task(name="gateway.jobs.tasks.drop_expired_partitions")
def drop_expired_partitions() -> int:
    retention = get_settings().audit_retention_days
    with _pg() as conn:
        row = conn.execute("SELECT drop_expired_audit_partitions(%s)", (retention,)).fetchone()
    dropped = int(row[0]) if row else 0
    log.info("partitions_dropped", dropped=dropped, retention_days=retention)
    return dropped


@app.task(name="gateway.jobs.tasks.rollup_minutes")
def rollup_minutes(lookback_minutes: int = 5) -> int:
    with _pg() as conn:
        cursor = conn.execute(ROLLUP_SQL, {"lookback": lookback_minutes})
        rows = cursor.rowcount
    log.info("rollup_done", rows=rows, lookback_minutes=lookback_minutes)
    return rows


@app.task(name="gateway.jobs.tasks.replay_dlq")
def replay_dlq(max_records: int = 10_000) -> dict[str, int]:
    """Re-publish DLQ records that decode now (e.g. after a schema fix is deployed).

    Records that are still poison stay in the DLQ topic for inspection (we only advance
    this job's own consumer group), so the job never loops a bad record forever.
    """
    return asyncio.run(_replay_dlq(max_records))


async def _replay_dlq(max_records: int) -> dict[str, int]:
    s = get_settings()
    consumer = AIOKafkaConsumer(
        s.audit_dlq_topic, bootstrap_servers=s.kafka_bootstrap, group_id="dlq-replayer",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=s.kafka_bootstrap, acks="all")
    await consumer.start()
    await producer.start()
    replayed = still_poison = 0
    try:
        while replayed + still_poison < max_records:
            batches = await consumer.getmany(timeout_ms=2_000, max_records=500)
            records = [r for part in batches.values() for r in part]
            if not records:
                break
            for record in records:
                try:
                    decode(record.value or b"")
                except ValueError:
                    still_poison += 1
                    continue
                await producer.send_and_wait(
                    s.audit_topic, value=record.value, key=record.key,
                    headers=[h for h in record.headers or () if not h[0].startswith("dlq.")],
                )
                replayed += 1
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
    log.info("dlq_replay_done", replayed=replayed, still_poison=still_poison)
    return {"replayed": replayed, "still_poison": still_poison}
