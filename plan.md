# Implementation Plan — Multi-Provider API Gateway

> **Session handoff.** Nothing is built yet; this file is the complete design.
> Source task: `/home/admin01/Desktop/Task.pdf`. Start at §10 Build order.

## Context for a fresh session

- **Task:** backend service that ingests API requests, proxies them to 3 external mock
  providers behind **one** endpoint, and stores request/result metadata. Must handle
  1,000–10,000 RPS, be runnable with `docker compose up`, and ship an architecture
  diagram, a README, and benchmark results.
- **Providers:** A `http://92.114.51.251:8001/v1/process` (300–1000ms, high reliability,
  strict rate limit, 429s) · B `:8002` and C `:8003` (1000–10000ms, intermittent 5xx,
  timeouts, higher limits).
- **Provider reachability:** unreachable from this machine — all three ports time out,
  `ping` reports network unreachable (this box routes through a corporate proxy at
  `172.24.17.18:3228`, which does not reach that host either). User said they have a
  plan for this. Design keeps provider URLs env-driven so it does not matter.
- **Decisions already locked with the user** (do not re-litigate):
  FastAPI · Nginx · Redis · **Kafka for the audit stream** · **Postgres + Mongo split
  by role** · **Celery/RabbitMQ for scheduled jobs only** · Grafana · OTel/Jaeger ·
  idempotency keys · **no hedged requests** (unsafe to duplicate a non-idempotent
  `/process` call; tail latency handled by selection, breaker, and early shedding).

---

## 0. The two facts that drive every decision

**1. The endpoint is synchronous, so our p99 is floored by the provider's p99.**
We cannot be faster than upstream. We can only (a) pick a better upstream, (b) stop
waiting on a bad one, (c) never let upstream pain become our pain. Everything not
required to produce the caller's response is pushed off the request path entirely.

**2. Little's Law sets the real ceiling: `RPS = concurrency / latency`.**
At 10,000 RPS with ~1s upstream latency we need **10,000 concurrent upstream sockets**.
The bottleneck is never FastAPI — it is provider rate limits, file descriptors, and
connection pooling. This is stated explicitly in the README.

**Architectural principle, one sentence:**

> **Redis is the only datastore on the request path. Everything else lives behind the
> queue, so no database can ever slow down or fail a caller.**

---

## 1. Decisions and rationale

| Area | Decision | Why |
|---|---|---|
| API | FastAPI + uvicorn, 1 event loop per process, N replicas | A proxy is pure I/O-wait; one loop holds thousands of open upstream calls. Horizontal scaling by replica keeps metrics clean (no prometheus multiprocess shims) and matches a k8s deployment. |
| Balancer | Nginx, `least_conn` | One public port; spreads load; absorbs slow clients. |
| Coordination | Redis + Lua | Sub-ms, atomic, shared across replicas. The only store fast enough to touch per request. |
| Upstream client | httpx.AsyncClient, per-provider pool | Async, keep-alive, per-host connection limits. |
| Audit transport | **Kafka** | 10k/s of immutable facts needing replay and partition parallelism. A log, not a queue. |
| Summary store | **Postgres**, range-partitioned by day | Fixed narrow rows; SQL aggregation and joins; retention = `DROP TABLE` (instant). |
| Payload store | **MongoDB**, TTL index | Per-provider response shapes differ and will keep differing. Schema-free, no migration per provider. |
| Scheduled jobs | **Celery + RabbitMQ** | Low-volume *jobs* (partition rollover, rollups, DLQ replay) — exactly what Celery is good at. |
| Observability | structlog + Prometheus/Grafana + OTel/Jaeger | Trace ID through every layer; metrics per replica; sampled distributed traces. |
| Hedged requests | **Excluded** | `/v1/process` is not provably read-only and provider calls are typically billed per call. Duplicating is unsafe. Tail latency handled by selection + breaker + early shedding instead. |

### Rejected alternatives (documented in README)

- **Celery/RabbitMQ for the audit stream** — per-message routing, index and ack
  bookkeeping; degrades under backlog; one queue is one Erlang process; **no replay**.
  Great for jobs, wrong for a stream.
- **Redis Streams** — fast and free (Redis is already there), but memory-bound
  (retention in minutes), weaker durability (`appendfsync everysec` loses ~1s), and
  competes with rate-limiter traffic on Redis's single thread.
- **Postgres-only** — payloads become a fat variable `jsonb` column: TOAST overhead,
  bloated partitions, slow autovacuum.
- **ClickHouse** — the *correct* answer for sustained 10k RPS summary ingest, and the
  documented migration path. Omitted here because it is outside the suggested stack.

---

## 2. Request lifecycle

```
T+0        Nginx            terminate, balance across N api replicas
T+0.1ms    Middleware       accept X-Request-ID or mint ULID -> log ctx + OTel span
                            START DEADLINE CLOCK (now + 10s)
T+0.3ms    Admission        ONE pipelined Redis round-trip:
                              1. inbound token bucket (per API key)   -> 429
                              2. idempotency key lookup               -> cached result, 0 provider calls
                              3. local concurrency semaphore          -> 503 + Retry-After
                            Shedding happens HERE, before any work.
T+1ms      Validation       pydantic v2 -> CanonicalRequest
T+1ms      Selection        breaker state  : local cache, refreshed from Redis every 200ms
                            ewma + inflight: pure local memory
                            rate token     : Redis Lua, atomic, per request (limit is global)
                            -> power-of-two-choices on ewma_ms * (1 + inflight)
T+1.5ms    Upstream         adapter maps canonical -> provider payload (+ creds, +trace, +extra fields)
  ..10s                     httpx, timeout = min(provider_timeout, deadline.remaining())
                            <-- ~99.9% of wall clock lives here
           on failure       update local EWMA; breaker counters -> Redis (fire-and-forget)
                            429 -> AIMD shrink of that provider's effective rps
                            if safe_to_retry AND retry budget AND deadline remains:
                                reselect excluding failed provider, attempt again
T+~1s      Response         adapter maps provider -> canonical; SETEX idempotency result
                            *** CALLER IS DONE ***
T+~1s      Audit emit       audit_buffer.put_nowait(event)   # bounded, O(1), never blocks
                            buffer full -> DROP + audit_dropped_total++
                            (we never make a caller wait to write a log line)
---------- off the request path ------------------------------------------------
bg 50ms    Producer         drain buffer -> Kafka batched (linger.ms=50, lz4,
                            key=trace_id, topic audit.requests, 12 partitions)
bg         Consumer         read ~1000 -> fan out:
                              Postgres: COPY to staging -> INSERT ON CONFLICT DO NOTHING
                              Mongo   : insert_many(ordered=False), _id = trace_id
                            commit offset only after BOTH land (at-least-once + dedupe)
periodic   Celery beat      create tomorrow's partition, drop expired, hourly rollups, DLQ replay
```

**The payoff:** if Postgres dies, the consumer stops committing offsets, Kafka retains
the data, and the consumer catches up on recovery. The caller never knew.

---

## 3. Core mechanisms

### 3.1 Provider selection — power-of-two-choices

Eligible = breaker not OPEN **and** bulkhead has room.
Sample 2 eligible at random, pick lower `ewma_latency_ms * (1 + inflight)`.

Deterministic "pick the best" causes herding — every replica stampedes the same
provider the moment it looks good. P2C is what Finagle and Envoy do.

EWMA is updated per worker from its own observations (`alpha ~ 0.2`); no coordination
cost. Cold start seeds from configured expected latency.

### 3.2 Outbound rate limiting — Redis token bucket + AIMD

`token_bucket.lua(key, rate, burst, now, cost) -> (allowed, retry_after_ms)`
Atomic, one round-trip. Per provider, shared across all replicas — the provider's
limit is global, so this is the one thing that cannot be local.

**Adaptive (AIMD)** — discovers Provider A's real limit instead of guessing it:
- on `429`: `effective_rps = max(min_rps, effective_rps * 0.5)`, honor `Retry-After`
- on a clean window: `effective_rps = min(configured_rps, effective_rps + step)`

### 3.3 Circuit breaker

Per provider, 10 × 1s rolling buckets in Redis.
OPEN when `failures/total > 0.5` **and** `total >= 20` (minimum-volume gate stops a
single failure in a quiet window from tripping it).
OPEN → cooldown 5s → HALF_OPEN, where a Redis `SETNX` lock with TTL ensures **exactly
one replica** sends the probe. Success closes it; failure reopens.

Read from a local cache refreshed every 200ms — 3 providers × 10k RPS would be 30k
Redis ops/sec for data that does not need millisecond freshness.

### 3.4 Retry policy — classified by what the failure *proves*

| Failure | Proves | Retry |
|---|---|---|
| Connection refused / DNS | request never arrived | yes |
| HTTP 429 | explicitly rejected, not processed | yes |
| HTTP 502 / 503 | rejected at edge, not processed | yes |
| **Read timeout** | **nothing — may be mid-processing** | only if op is idempotent or provider honors our idempotency key |
| HTTP 500 after send | unknown | same |

> "Connect-timeout is safe to retry, read-timeout is not."

We forward our `Idempotency-Key` downstream so providers that support it make
read-timeouts safe. Policy is per-operation and config-driven.

**Retry budget** (Google SRE): retries capped at ~10% of traffic in a rolling Redis
window. Without it, an upstream brownout triples your own load at the worst moment.
This is what actually satisfies "degradations must not cascade."

### 3.5 Bulkhead and deadline

- Per-provider `asyncio.Semaphore` — one slow provider cannot consume all event-loop
  capacity and starve the others.
- One `Deadline` created at ingress. Every attempt timeout is
  `min(provider_timeout, deadline.remaining())`. Stop retrying when
  `remaining() < MIN_ATTEMPT_MS`. The caller's SLA is never exceeded by our own retries.

### 3.6 Load shedding

Per-worker in-flight semaphore checked at admission. Over threshold → immediate
`503 + Retry-After`. **Fail fast beats fail slow** — a request queued past its deadline
is pure waste that also hurts everyone behind it.

### 3.7 Degrade-open

If Redis is unreachable, fall back to **per-worker local** rate limits and local
breaker state rather than rejecting traffic. Availability over perfect coordination.
Metric + loud log when degraded.

### 3.8 Provider adapters

`ProviderAdapter` ABC: `build_request(canonical) -> httpx.Request` and
`parse_response(raw) -> CanonicalResponse`, plus `classify_error(exc_or_response)`.
Injects credentials, the trace header, and any provider-specific extra fields.

**Adding provider D = one class + one config block.** This is the abstraction that
matches the real business of normalizing heterogeneous identity/banking APIs.

---

## 4. Data model

### Postgres — summary (one narrow row, ~200 bytes)

```sql
CREATE TABLE audit_request (
  received_at     timestamptz NOT NULL,
  trace_id        text        NOT NULL,
  tenant_id       text,
  idempotency_key text,
  provider        text,
  attempt_count   smallint,
  status          text,      -- ok | provider_error | rate_limited | shed | deadline_exceeded
  http_code       smallint,
  total_ms        integer,
  provider_ms     integer,
  error_class     text,
  PRIMARY KEY (received_at, trace_id)
) PARTITION BY RANGE (received_at);
```

Daily partitions created ahead by Celery beat; retention = `DROP TABLE` (instant,
no `DELETE`, no vacuum storm).

**Load path:** `COPY` into an `UNLOGGED` staging table → `INSERT ... SELECT ...
ON CONFLICT DO NOTHING`. Gives COPY speed *and* idempotent at-least-once loading.

**Rollups:** `audit_rollup_1m(bucket, provider, status, count, p50, p95, p99)` built by
Celery so dashboards never scan raw partitions.

### MongoDB — payload (variable shape, KBs)

```json
{ "_id": "<trace_id>",
  "received_at": "...",
  "request": { ... },
  "attempts": [
    { "provider": "A", "sent_body": {...}, "raw_response": {...}, "latency_ms": 412 },
    { "provider": "C", "sent_body": {...}, "raw_response": {...}, "latency_ms": 1180 }
  ] }
```

`_id = trace_id` makes dedupe free (duplicate insert fails harmlessly under
`ordered=False`). TTL index on `received_at`. Shard key `hash(_id)` when scaled out.

`raw_response` has a **different shape per provider** — the entire reason this is not
a Postgres column.

### Kafka

`audit.requests` — 12 partitions, key `trace_id`, lz4, retention 24h.
`audit.requests.dlq` — poison records, replayed by a Celery job.

---

## 5. Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/process` | the proxied endpoint |
| GET | `/v1/requests/{trace_id}` | audit lookup — joins Postgres summary + Mongo payload. Proves the persistence layer is actually useful, not write-only. |
| GET | `/v1/providers` | live provider state: breaker, EWMA, effective RPS, inflight. Demo + Grafana. |
| GET | `/healthz` | liveness — 200 while the process is alive |
| GET | `/readyz` | readiness — Redis reachable, Kafka producer connected |
| GET | `/metrics` | Prometheus |

---

## 6. Observability

**Tracing.** Accept `X-Request-ID` / `traceparent`, else mint a ULID. Bound to
structlog contextvars, attached to the OTel span, forwarded to the provider, returned
in the response header, and carried through Kafka into the consumer's span — so one
trace spans ingress → provider → persistence.

**Logs.** structlog JSON to stdout; every line carries `trace_id`, `provider`,
`attempt`, `outcome`, `latency_ms`.

**Metrics.**
- `http_request_duration_seconds` (histogram, by route/status)
- `provider_attempt_duration_seconds` (histogram, by provider/outcome)
- `provider_attempts_total{provider,outcome}`
- `breaker_state{provider}` (gauge 0/1/2)
- `provider_effective_rps{provider}` (gauge — shows AIMD working)
- `admission_rejected_total{reason}` (rate_limit | shed | deadline)
- `retry_budget_exhausted_total`
- `audit_buffer_depth`, `audit_dropped_total`, `kafka_produce_errors_total`
- `consumer_batch_rows`, `consumer_lag` (from Kafka consumer group)

Prometheus scrapes replicas via `dns_sd_configs` (`names: [api]`, type A) so
`--scale api=N` needs no config change.

**Grafana** ships with a provisioned dashboard: offered vs served RPS, latency
percentiles, provider distribution, breaker timeline, shed rate, consumer lag.

---

## 7. Topology (`docker compose up`)

```
nginx ──► api × N ──► redis
                 ├──► kafka (KRaft, single broker) ──► consumer × M ──► postgres
                 │                                                  └─► mongo
                 └──► mock-a / mock-b / mock-c
rabbitmq ──► celery-worker + celery-beat ──► postgres
prometheus ──► grafana        jaeger        k6 (profile: bench)
```

Compose profiles keep it manageable: default brings up everything needed to serve and
persist; `--profile bench` adds the load generator.

**Mock providers.** A local trio reproducing the documented A/B/C profiles (latency
bands, 429 on A, intermittent 5xx and timeouts on B/C). Provider URLs are env-driven,
so pointing at the real hosts is a one-line `.env` change. Rationale: `docker compose
up` must work standalone, and benchmarking 10k RPS across the public internet to a
single remote box measures *their network*, not our service.

---

## 8. Benchmark methodology

**Tool: k6, `constant-arrival-rate` executor.** Open model — a closed-model generator
(fixed VUs) silently hides queueing by slowing its own request rate when the system
degrades. Open model is the only honest way to measure a system under overload.

**Two configurations, because they answer different questions:**

1. **Realistic** — mocks at documented latency profiles. End-to-end truth. Throughput
   here is bounded by provider concurrency, exactly as Little's Law predicts.
2. **Overhead isolation** — mocks pinned at 1ms. Measures the gateway's *own* ceiling
   and per-request overhead with upstream latency removed from the equation.

**Ramp:** 100 → 500 → 1,000 → 2,000 RPS, plus a 10-minute soak at a steady rate to
confirm no memory growth and that the consumer keeps up with the producer.

**Recorded:** offered vs achieved RPS, avg / p95 / p99 latency, error rate broken down
by class (502 / 503 / 504 / 429), shed rate, provider distribution, Kafka consumer lag,
and per-container CPU/memory. k6 `thresholds` make each run self-validating.

---

## 9. Known bottlenecks (a stated deliverable)

1. **Provider concurrency × latency ceiling.** The hard limit. 10k RPS at 1s upstream
   latency = 10k concurrent sockets. Quantified in the README.
2. **Redis is single-threaded and a SPOF.** ~1–2 Lua calls per request; 10k RPS is
   comfortable (Redis does 100k+ ops/s), but it is a single point of failure →
   mitigated by degrade-open, and by Redis Cluster / per-provider sharding at scale.
3. **Python GIL and JSON cost.** The proxy is I/O-bound so asyncio is right, but
   serialization is real CPU → `orjson`, and scale by processes.
4. **Postgres at sustained 10k rows/s.** Achievable with COPY + partitioning, but you
   fight WAL, autovacuum, and index maintenance → **ClickHouse migration path**.
5. **Single Kafka broker in compose.** Not HA; production needs RF≥3.
6. **File descriptors.** 10k concurrent upstream sockets requires `ulimit -n` tuning
   and ephemeral port range awareness.

---

## 10. Build order

| # | Milestone | Contents |
|---|---|---|
| 1 | Skeleton runs | compose with nginx + api + redis + mocks; `POST /v1/process` proxies to one provider; health endpoints |
| 2 | Multi-provider core | adapters, registry, selector (P2C), bulkhead, deadline, dispatcher loop |
| 3 | Resilience | Redis Lua token bucket + AIMD, circuit breaker, retry classification + budget, degrade-open |
| 4 | Admission | inbound rate limit, idempotency keys, load shedding |
| 5 | Audit path | bounded buffer, Kafka producer, consumer with Postgres COPY + Mongo bulk, DLQ |
| 6 | Jobs | Celery + RabbitMQ: partition rollover, retention, rollups, DLQ replay |
| 7 | Observability | structlog, Prometheus metrics, OTel/Jaeger, Grafana dashboard |
| 8 | Lookup endpoints | `/v1/requests/{trace_id}`, `/v1/providers` |
| 9 | Tests | unit: selector, breaker, limiter, retry classification, deadline. integration: full smoke |
| 10 | Benchmark + docs | k6 scripts, run, record results, architecture diagram, README |

---

## 11. Deliverables mapping

| Required | Where |
|---|---|
| Runnable via one command | `docker compose up` |
| Architecture diagram | `docs/architecture.md` + rendered diagram |
| Setup / run / benchmark instructions | README §Quickstart, §Benchmark |
| Architectural overview + rationale | README §Architecture, this plan §1 |
| Trade-offs and known bottlenecks | README §Trade-offs, this plan §1 and §9 |
| Scaling + persistence strategy 1k–10k RPS | README §Scaling — Little's Law, replica math, partitioning, ClickHouse path |
| Benchmark findings | README §Results, raw k6 output in `bench/results/` |
