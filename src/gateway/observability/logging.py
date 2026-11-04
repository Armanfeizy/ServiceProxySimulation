"""structlog JSON to stdout. Every line carries the bound context (trace_id, ...)."""

from __future__ import annotations

import logging
import sys

import orjson
import structlog


def _orjson_dumps(obj: object, default: object = None) -> bytes:
    return orjson.dumps(obj, default=str)


def setup_logging(level: str, service: str) -> None:
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    structlog.configure(
        processors=[
            *shared,
            _add_service(service),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=_orjson_dumps),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelNamesMapping()[level.upper()]),
        logger_factory=structlog.BytesLoggerFactory(sys.stdout.buffer),
        cache_logger_on_first_use=True,
    )

    # Route stdlib loggers (uvicorn, aiokafka, celery) through the same JSON shape.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared, _add_service(service)],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("uvicorn.access", "aiokafka", "kafka", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _add_service(service: str) -> structlog.types.Processor:
    def processor(
        _: object, __: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        event_dict.setdefault("service", service)
        return event_dict

    return processor
