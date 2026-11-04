"""OpenTelemetry tracing: ingress -> provider attempts -> Kafka -> persistence.

Our trace_id is a ULID, which is 128 bits — exactly an OTel trace id. Unless the caller
sent a `traceparent` to continue, a custom id generator reuses it, so the `X-Trace-ID`
header, every log line and Jaeger all carry the same id (Jaeger shows it as 32 hex chars).
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager

from opentelemetry import propagate, trace
from opentelemetry.sdk.trace import IdGenerator, RandomIdGenerator, TracerProvider
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

_tracer = trace.get_tracer("gateway")
_minted_trace_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "minted_trace_id", default=None
)


class RequestIdAwareIdGenerator(IdGenerator):
    def __init__(self) -> None:
        self._random = RandomIdGenerator()

    def generate_span_id(self) -> int:
        return self._random.generate_span_id()

    def generate_trace_id(self) -> int:
        minted = _minted_trace_id.get()
        return minted if minted else self._random.generate_trace_id()


def setup_tracing(*, enabled: bool, service: str, endpoint: str, sample_ratio: float) -> None:
    if not enabled:
        return
    import requests
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    session = requests.Session()
    session.trust_env = False  # never send telemetry through an HTTP proxy
    provider = TracerProvider(
        resource=Resource.create({"service.name": service}),
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        id_generator=RequestIdAwareIdGenerator(),
    )
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces", session=session),
            max_queue_size=8192,
            max_export_batch_size=512,
        )
    )
    trace.set_tracer_provider(provider)


def shutdown_tracing() -> None:
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.shutdown()


@contextmanager
def server_span(name: str, headers: Mapping[str, str], trace_id_int: int) -> Iterator[Span]:
    parent = propagate.extract(headers)
    has_parent = trace.get_current_span(parent).get_span_context().is_valid
    token = _minted_trace_id.set(None if has_parent else trace_id_int)
    try:
        with _tracer.start_as_current_span(name, context=parent, kind=SpanKind.SERVER) as span:
            yield span
    finally:
        _minted_trace_id.reset(token)


@contextmanager
def client_span(provider: str, url: str, attempt: int) -> Iterator[Span]:
    with _tracer.start_as_current_span(
        f"provider {provider}",
        kind=SpanKind.CLIENT,
        attributes={"provider.name": provider, "url.full": url, "attempt": attempt},
    ) as span:
        yield span


def finish_client_span(span: Span, outcome: str, status_code: int | None) -> None:
    span.set_attribute("provider.outcome", outcome)
    if status_code is not None:
        span.set_attribute("http.response.status_code", status_code)
    if outcome != "success":
        span.set_status(Status(StatusCode.ERROR, outcome))


def inject(carrier: MutableMapping[str, str]) -> None:
    propagate.inject(carrier)


def carrier_for_current() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def extract_link(carrier: Mapping[str, str]) -> trace.Link | None:
    ctx = trace.get_current_span(propagate.extract(carrier)).get_span_context()
    return trace.Link(ctx) if ctx.is_valid else None


def tracer() -> trace.Tracer:
    return _tracer


__all__ = [
    "carrier_for_current",
    "client_span",
    "extract_link",
    "finish_client_span",
    "inject",
    "server_span",
    "setup_tracing",
    "shutdown_tracing",
    "tracer",
]
