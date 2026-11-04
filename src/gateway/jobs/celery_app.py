"""Celery + RabbitMQ for low-volume scheduled *jobs* — never for the audit stream.

Run:
  celery -A gateway.jobs.celery_app worker --loglevel INFO
  celery -A gateway.jobs.celery_app beat   --loglevel INFO
"""

from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab
from celery.signals import after_setup_logger, worker_ready

from gateway.observability.logging import setup_logging

app = Celery(
    "gateway-jobs",
    broker=os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//"),
    include=["gateway.jobs.tasks"],
)
app.conf.update(
    task_ignore_result=True,
    task_acks_late=True,                 # a job killed mid-run is redelivered
    worker_prefetch_multiplier=1,
    task_time_limit=300,
    task_soft_time_limit=240,
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,
    beat_schedule={
        "ensure-partitions": {
            "task": "gateway.jobs.tasks.ensure_partitions",
            "schedule": crontab(minute=5),                 # hourly; creates 3 days ahead
        },
        "drop-expired-partitions": {
            "task": "gateway.jobs.tasks.drop_expired_partitions",
            "schedule": crontab(hour=0, minute=30),
        },
        "rollup-1m": {
            "task": "gateway.jobs.tasks.rollup_minutes",
            "schedule": 60.0,
        },
        "replay-dlq": {
            "task": "gateway.jobs.tasks.replay_dlq",
            "schedule": 600.0,
        },
    },
)


@after_setup_logger.connect
def _json_logs(**_: object) -> None:
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"), "celery")


@worker_ready.connect
def _bootstrap(**_: object) -> None:
    # Don't wait up to an hour for the first partition check after a fresh start.
    app.send_task("gateway.jobs.tasks.ensure_partitions")
