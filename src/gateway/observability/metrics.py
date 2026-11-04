"""Prometheus metrics. One process per container, so no multiprocess shims are needed."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Latencies span 1ms (overhead mode) to 10s (provider B/C tail): cover both ends.
LATENCY_BUCKETS = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75,
    1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0,
)

# --- ingress -----------------------------------------------------------------------
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds", "End-to-end request latency at the gateway",
    ["route", "status"], buckets=LATENCY_BUCKETS,
)
INFLIGHT_REQUESTS = Gauge("gateway_inflight_requests", "Requests currently in flight (replica)")
EVENT_LOOP_LAG = Gauge("event_loop_lag_seconds", "EWMA of event-loop scheduling delay (replica)")
ADMISSION_REJECTED = Counter(
    "admission_rejected_total", "Requests refused before any upstream work", ["reason"],
)
IDEMPOTENT_REPLAYS = Counter("idempotent_replays_total", "Responses served from idempotency cache")

# --- upstream ----------------------------------------------------------------------
PROVIDER_ATTEMPT_DURATION = Histogram(
    "provider_attempt_duration_seconds", "Latency of a single upstream attempt",
    ["provider", "outcome"], buckets=LATENCY_BUCKETS,
)
PROVIDER_ATTEMPTS = Counter("provider_attempts_total", "Upstream attempts", ["provider", "outcome"])
PROVIDER_INFLIGHT = Gauge("provider_inflight", "Upstream calls in flight (replica)", ["provider"])
PROVIDER_EWMA_MS = Gauge("provider_ewma_latency_ms", "EWMA latency seen by this replica", ["provider"])
BREAKER_STATE = Gauge("breaker_state", "0=closed 1=half_open 2=open", ["provider"])
PROVIDER_EFFECTIVE_RPS = Gauge(
    "provider_effective_rps", "AIMD-adjusted outbound rate limit (global)", ["provider"],
)
UPSTREAM_UNAVAILABLE = Counter(
    "upstream_unavailable_total", "Requests failed with no provider able to take them", ["reason"],
)
RETRIES = Counter("retries_total", "Retries sent to another provider", ["from_provider"])
RETRY_BUDGET_EXHAUSTED = Counter("retry_budget_exhausted_total", "Retries denied by the budget")
COORDINATION_DEGRADED = Gauge(
    "coordination_degraded", "1 when Redis is unreachable and replicas run on local limits",
)

# --- audit producer ------------------------------------------------------------------
AUDIT_BUFFER_DEPTH = Gauge("audit_buffer_depth", "Audit events waiting for Kafka")
AUDIT_DROPPED = Counter("audit_dropped_total", "Audit events dropped (buffer full)", ["reason"])
KAFKA_PRODUCE_ERRORS = Counter("kafka_produce_errors_total", "Failed Kafka deliveries")
AUDIT_PRODUCED = Counter("audit_produced_total", "Audit events handed to Kafka")
AUDIT_REQUEUED = Counter("audit_requeued_total", "Audit events put back after a failed delivery")

# --- audit consumer ------------------------------------------------------------------
CONSUMER_BATCH_ROWS = Histogram(
    "consumer_batch_rows", "Rows per persisted batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000, 2000, 5000),
)
CONSUMER_BATCH_SECONDS = Histogram(
    "consumer_batch_seconds", "Time to persist one batch", ["sink"], buckets=LATENCY_BUCKETS,
)
CONSUMER_ROWS = Counter("consumer_rows_total", "Rows persisted", ["sink"])
CONSUMER_WRITE_ERRORS = Counter("consumer_write_errors_total", "Failed batch writes", ["sink"])
CONSUMER_DLQ = Counter("consumer_dlq_total", "Poison records sent to the DLQ")
CONSUMER_LAG = Gauge("consumer_lag", "Records behind the log end", ["partition"])
