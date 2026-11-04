# Multi-Provider API Gateway

One endpoint, `POST /v1/process`, in front of three unreliable upstream providers. The
gateway picks a provider per request, keeps upstream failures from cascading, protects
itself and the providers from overload, and records every request through an
asynchronous audit pipeline built for 1,000–10,000 RPS.

> **Principle.** Redis is the only datastore on the request path. Everything else lives
> behind a queue, so no database can slow down or fail a caller.

- Architecture diagrams: [`docs/architecture.md`](docs/architecture.md)
- Design notes and rationale: [`plan.md`](plan.md)

---

## Quickstart

```bash
docker compose up -d --build          # whole stack, 4 api replicas, local mock providers
curl -s localhost:8088/v1/process -H 'content-type: application/json' -d '{"amount": 42}' | jq
```

| URL | What |
|---|---|
| http://localhost:8088 | the gateway (Nginx) |
| http://localhost:3300 | Grafana, dashboard "Gateway overview" (no login) |
| http://localhost:16686 | Jaeger traces |
| http://localhost:9095 | Prometheus |
| http://localhost:15673 | RabbitMQ management (gateway / gateway) |

Scale the api tier: `docker compose up -d --scale api=8` (Nginx and Prometheus discover
replicas through Docker DNS; nothing to edit).

**Real providers instead of mocks:** copy `.env.example` to `.env` and set
`PROVIDER_A_URL`, `PROVIDER_B_URL`, `PROVIDER_C_URL` (for example
`http://92.114.51.251:8001/v1/process`). The local mocks reproduce the documented
profiles so `docker compose up` works standalone and benchmarks measure the gateway, not
the public internet. Mock behaviour can be changed live, e.g. make B fail to watch the
breaker open:

```bash
docker compose exec mock-b python -c "import urllib.request,json; \
  urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8000/admin/config', \
  data=json.dumps({'error_rate': 1.0}).encode(), method='POST'))"
```

### API

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/process` | proxied endpoint; body is any JSON object |
| GET | `/v1/requests/{trace_id}` | audit lookup: Postgres summary + Mongo payload |
| GET | `/v1/providers` | live provider state: breaker, EWMA, effective RPS, in-flight |
| GET | `/healthz` · `/readyz` · `/metrics` | liveness · readiness · Prometheus |

Request headers: `X-API-Key` (tenant for inbound rate limiting), `Idempotency-Key`
(replays the stored answer, 409 while in flight, 422 if reused with another body),
`X-Request-ID` (stored as `client_request_id` and echoed back), `traceparent`.

Every response carries `X-Trace-ID`, a ULID minted by the gateway. The same id appears in
every log line, in Jaeger and in the audit store.

```json
{"trace_id": "01JC…", "provider": "A", "attempts": 1, "latency_ms": 512.4, "result": {…}}
{"trace_id": "01JC…", "error": {"code": "deadline_exceeded", "message": "…", "attempts": […]}}
```

| Status | Meaning |
|---|---|
| 200 | a provider answered successfully |
| 4xx (passthrough) | the provider rejected the input itself; never retried |
| 409 / 422 | idempotency conflict / key reused with another body |
| 413 / 422 | body too large / not a JSON object |
| 429 | this API key exceeded its inbound rate |
| 502 | upstream failed and no safe retry was possible |
| 503 + Retry-After | shed (gateway at capacity) or every provider's capacity is spent |
| 504 | upstream did not answer within the 10 s deadline |

### Tests

```bash
python3.13 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest tests/unit                                   # pure + in-process providers
docker run --rm -d -p 6390:6379 redis:7-alpine && .venv/bin/pytest tests/integration/test_lua.py
docker compose up -d && .venv/bin/pytest tests/integration/test_smoke.py
```

---

## Architecture

_See [`docs/architecture.md`](docs/architecture.md) for the component and sequence diagrams._

**Request path (everything a caller waits for):**

1. **Nginx** (`least_conn`, upstream keep-alive) spreads load over N api replicas.
2. **Middleware** mints the trace id, starts the 10 s deadline clock and opens the span.
3. **Admission**, before any work: local load shedding (503); then one pipelined Redis
   round-trip for the inbound token bucket (429) and the idempotency check.
4. **Selection:** power-of-two-choices over eligible providers (breaker not open, not
   inside a 429 back-off window, bulkhead has room), scored by `EWMA latency × (1 +
   in-flight)`. Then one Redis Lua call takes a token from that provider's *global* rate
   limit; if it is spent, the next-best provider is tried.
5. **Attempt** with `timeout = min(provider timeout, deadline remaining)`. The failure is
   classified by what it *proves* (see below). A retry goes to a different provider,
   only if safe and within the retry budget.
6. **Response**, idempotency result stored, then the audit event goes into a bounded
   in-memory buffer (`put_nowait`, O(1)). The caller is done at this point.

**Off the request path:** buffer → Kafka (batched, lz4, keyed by trace id) →
audit-consumer → Postgres `COPY` + Mongo bulk insert → offsets committed after both
sinks hold the batch. Celery beat drives partition rollover, retention, per-minute rollups
and DLQ replay.

### Resilience mechanisms

| Mechanism | Where | Behaviour |
|---|---|---|
| Circuit breaker | Redis (Lua), cached locally every 200 ms | 10 × 1 s buckets; opens at > 50 % failures with ≥ 20 requests; 5 s cooldown; half-open lets **exactly one** replica probe (`SET NX PX`) |
| Outbound rate limit | Redis token bucket (Lua, server time) | global across replicas because the provider's limit is global |
| AIMD | Redis (Lua) | 429 halves the effective rate (once per window, however many replicas saw it); each clean window adds 5 %; `Retry-After` blocks the provider for that long |
| Bulkhead | per replica | per-provider concurrency cap: one slow provider cannot absorb the event loop |
| Deadline | per request | one 10 s budget shared by all attempts; no attempt starts with < 150 ms left |
| Retry budget | per replica | retries ≤ 10 % of recent requests (+ small floor) so a brownout cannot multiply load |
| Load shedding | per replica | over `MAX_INFLIGHT` → immediate 503 + Retry-After; fail fast beats fail slow |
| Loop-lag shedding | per replica | event loop running > 200 ms late (CPU starvation) → shed a growing share of new requests, all of them at 400 ms |
| Don't blame the provider | per replica | a timeout counts against a breaker only if the attempt got its full timeout *and* our loop was keeping up |
| Degrade-open | per replica | Redis unreachable → local breakers and local buckets sized `limit / replicas` |

**Retry classification, by what the failure proves:**

| Failure | Proves | Retried |
|---|---|---|
| connect refused / connect timeout / pool timeout | never arrived | yes |
| 429, 502, 503, 408 | refused, not processed | yes |
| read timeout, 500, 504, broken response | nothing: it may be mid-processing | only if `OPERATION_IDEMPOTENT=true` or the provider honours our forwarded `Idempotency-Key` |
| other 4xx | the input is bad | no, passed through |

Hedged requests are deliberately excluded: `/v1/process` is not provably read-only and
provider calls are usually billed, so duplicating them is unsafe. Tail latency is handled
by selection, the breaker and early shedding instead.

### Technology choices

| Area | Choice | Why |
|---|---|---|
| API | FastAPI + uvicorn (uvloop, httptools), 1 loop per container, N replicas | a proxy is pure I/O wait; one loop holds thousands of open upstream calls; scaling by replica keeps metrics simple |
| Upstream client | aiohttp, one pool per provider, behind a small `Transport` interface | O(1) pool acquire at thousands of open sockets (httpx was measured at 62% of CPU, see below); proxies are explicit config, never inherited |
| Coordination | Redis + Lua | sub-millisecond and atomic; the only store fast enough to touch per request |
| Audit transport | Kafka (KRaft) | 10k/s of immutable facts that need replay and partition parallelism: a log, not a queue |
| Summary store | Postgres, daily range partitions | narrow fixed rows; SQL aggregation; retention is `DROP TABLE` |
| Payload store | MongoDB, TTL index | response shapes differ per provider and will keep changing |
| Jobs | Celery + RabbitMQ | low-volume scheduled *jobs* are exactly what Celery is for |
| Observability | structlog JSON, Prometheus/Grafana, OpenTelemetry → Jaeger | one trace id across logs, metrics and traces |

**Rejected:** Celery/RabbitMQ for the audit stream (per-message bookkeeping, degrades under
backlog, no replay); Redis Streams (memory-bound retention, weaker durability, competes
with the rate limiter on one thread); Postgres-only (fat `jsonb` payloads cause TOAST
bloat and slow vacuum); ClickHouse (the right answer for sustained 10k rows/s of
summaries, and the documented migration path, but outside the suggested stack).

---

## Persistence design

**Postgres `audit_request`**: one ~200-byte row per request, PK `(received_at,
trace_id)`, range-partitioned by day (partitions created 3 days ahead by Celery and by the
consumer at startup; a DEFAULT partition catches strays). The consumer loads each batch
with `COPY` into a session temp table, then `INSERT … SELECT … ON CONFLICT DO NOTHING`:
COPY speed, and replayed records are harmless. `audit_rollup_1m` holds per-minute
p50/p95/p99 per provider and status, recomputed over a 5-minute lookback so late rows
are counted.

**MongoDB `audit_payloads`**: one document per request with `_id = trace_id`, the
(size-capped) request body and every attempt's raw provider response. Duplicate inserts
fail harmlessly under `ordered=False`. TTL index expires payloads after 7 days.

**Kafka**: `audit.requests` has 12 partitions, keyed by trace id, retained 24 h.
`audit.requests.dlq` holds undecodable records; the replay job re-publishes those that
decode after a fix and never loops on ones that still don't.

**Lookup** (`GET /v1/requests/{id}`): the trace id is a ULID, so its embedded timestamp
prunes to one daily partition and the query is a primary-key lookup.

---

## Scaling from 1,000 to 10,000 RPS

**Little's Law sets the ceiling, not FastAPI:** `concurrency = RPS × latency`. At
10,000 RPS with ~1 s upstream latency the system holds **10,000 concurrent upstream
sockets**; with providers B/C averaging ~5.5 s it would be ~55,000. The real limits are
provider rate limits, sockets and file descriptors.

| Tier | 1k RPS | 10k RPS | Notes |
|---|---|---|---|
| api replicas | 4–6 | 20–25 | measured ~475 RPS of gateway overhead per replica on a busy shared host (see benchmark); also size `MAX_INFLIGHT` × replicas ≥ RPS × upstream latency |
| Nginx | 1 | 1–2 | keep-alive to upstream; `worker_connections` 65k |
| Redis | 1 | 1, then Cluster | ~2 Lua calls per request = 20k ops/s, far below Redis's ~100k+; keys are hash-tagged per provider so they shard cleanly |
| Kafka partitions | 12 | 24–48, RF 3 | consumer parallelism = partitions |
| audit-consumer | 1–2 | 6–12 | each batch is one COPY + one bulk insert |
| Postgres | 1 | 1 primary, tuned WAL | ~10k rows/s is achievable with COPY + partitioning; beyond that, move summaries to ClickHouse |
| MongoDB | 1 | sharded on `hashed(_id)` | payload volume is the bulk of bytes |

File descriptors: every container runs with `nofile=65536` (Nginx 200k). At very high
concurrency, also widen the ephemeral port range and enable `tcp_tw_reuse` on the host.

---

## Trade-offs and known bottlenecks

1. **Provider concurrency × latency** is the hard ceiling (above). The gateway can choose
   a better upstream and stop waiting on a bad one; it cannot be faster than upstream.
2. **Redis is a single point of failure.** Mitigated by degrade-open (local limits,
   availability over perfect coordination); production would run Sentinel or Cluster.
   Requests that carry an `Idempotency-Key` fail closed (503) while Redis is down, because
   exactly-once cannot be promised without it.
3. **Python CPU.** The path is I/O-bound, but JSON, logging and metrics are real CPU per
   request. orjson and a pure-ASGI middleware help; beyond that, scale by processes.
4. **Postgres at sustained 10k rows/s** fights WAL, autovacuum and index maintenance;
   ClickHouse is the migration path.
5. **Single Kafka broker** in compose: not HA; production needs RF ≥ 3.
6. **Audit is best-effort at the very edge.** If Kafka is unreachable long enough to
   fill the bounded buffer, events are dropped and counted (`audit_dropped_total`), not
   queued until the callers suffer. Durability starts once Kafka acknowledges.
7. **P2C with only three, very different providers.** Sampling two of three means the
   slow pair (B, C) gets the request whenever A is not in the sample, so at low load about
   a third of traffic goes to B/C even though A is idle (median 0.8 s, mean 2.3 s at
   100 RPS). That buys spread and fresh latency estimates for every provider, and at
   realistic load it does not matter because A's rate limit is the binding constraint. With
   more providers, or if low-load latency matters more, use full least-score selection
   below a few providers.
8. **Loop-lag shedding is a CPU-starvation signal, so it also fires when the *host* is
   busy**, not only this process. That is the intended behaviour (the replica genuinely
   cannot answer on time), but on a shared machine it makes results depend on neighbours.
9. **Retry budget and EWMA are per replica.** Nginx balances evenly, so each replica's
   view is representative; that saves a Redis round-trip per request.
10. **Readiness reports dependencies but does not fail on them**: failing readiness on
   Redis or Kafka would let an audit-path outage take down serving.

---

## Benchmark

### How to run

```bash
# Realistic: mocks at the documented latency/error/limit profiles, 6 api replicas
API_REPLICAS=6 docker compose up -d
docker compose --profile bench run --rm -e MODE=realistic -e STAGES=100,500,1000 \
    -e STAGE_SECONDS=60 -e MAX_VUS=7000 -e PREALLOC_SECONDS=5 k6

# Overhead isolation: mocks answer in ~1 ms with no errors and no limits
API_REPLICAS=6 docker compose --env-file bench/overhead.env up -d --force-recreate mock-a mock-b mock-c api
docker compose --profile bench run --rm -e MODE=overhead -e STAGES=500,1000,2000,3000 \
    -e STAGE_SECONDS=45 -e MAX_VUS=3000 -e PREALLOC_SECONDS=0.2 k6
```

k6 uses the `constant-arrival-rate` executor, an *open* model: it keeps offering the
target rate even when the system slows down, so queueing and overload show up in the
numbers instead of being hidden by a generator that backs off. Each run is
self-validating (thresholds) and writes JSON + Markdown to `bench/results/`. The k6
service deliberately has no `depends_on`, so `compose run k6` never recreates or rescales
the stack under test.

`dropped` counts iterations k6 itself could not start (no free VU in time). That is a
load-generator limit, not a gateway response.

### Environment

All numbers come from one shared 12-core / 32 GB workstation running the whole stack
*and* k6 *and* about 40 unrelated containers from other projects. During the runs the
load average was 11–22 and swap was full. Absolute numbers are therefore pessimistic, and
the 1,000 RPS realistic stage is sensitive to neighbours: an earlier run at the same
settings under a heavier host load shed far more. Treat the shape as the finding and the
absolute values as a floor.

### Results: realistic profile

Mocks at the documented profiles (A 300–1000 ms with a 100 RPS limit and 429s; B and C
1–10 s with 5% 5xx and 2% hangs; 1,000 RPS limits). 6 api replicas, 60 s per stage.

| Offered RPS | Achieved | p50 | avg | p95 | p99 | 2xx | 502 | 503 | 504 |
|---|---|---|---|---|---|---|---|---|---|
| 100 | 100 | 842 ms | 2.32 s | 8.77 s | 9.92 s | 98.7% | 0.7% | 0 | 0.7% |
| 500 | 495 | 5.23 s | 5.21 s | 9.83 s | 10.01 s | 94.9% | 1.6% | 0 | 3.5% |
| 1,000 | 976 | 5.61 s | 5.57 s | 9.94 s | 10.06 s | 93.8% | 1.7% | 0.3% | 4.2% |

- **Latency is the providers' latency.** p99 is pinned at the 10 s deadline at every
  stage: the gateway never makes a caller wait longer than its deadline.
- **Little's Law, measured:** peak in-flight was 5,491 concurrent requests at ~976 RPS ×
  5.6 s ≈ 5,470. This is the ceiling described in "Scaling": sockets, not CPU.
- **Errors are the injected provider faults**, after the retry policy: 504 = B/C hangs and
  answers that land past the deadline; 502 = 5xx that are not safe to retry (500, unknown
  outcome); 503 = 0.3% shed. No breaker opened; 3,576 safe retries, budget never
  exhausted.
- **AIMD found A's limit:** A's effective rate moved between 23 and 200 RPS (configured
  200, real 100). A carried ~10% of successes at 1,000 RPS because its limit binds.
- **The audit path kept up:** consumer lag ≤ 22 records, zero dropped events.

### Results: overhead isolation

Mocks at ~1 ms, no errors, no limits. This is the gateway's own cost: Nginx, admission
(Redis), selection (Redis), one upstream call, the audit emit, logs, metrics, traces.

| Offered RPS | Achieved | p50 | avg | p95 | p99 | Errors |
|---|---|---|---|---|---|---|
| 500 | 500 | 6.0 ms | 7.1 ms | 14.1 ms | 24.0 ms | 0 |
| 1,000 | 1,000 | 6.8 ms | 11.6 ms | 33.0 ms | 90.1 ms | 0 |
| 2,000 | 1,971 | 20.4 ms | 52.3 ms | 218.5 ms | 415.5 ms | 0 |
| 3,000 | 2,850 | 179.2 ms | 203.1 ms | 443.2 ms | 643.1 ms | 0.23% (shed) |

- **Ceiling on this host: ~2,850 RPS with 6 replicas (~475 RPS per replica)**; past that,
  loop-lag shedding answers with fast 503s instead of letting latency explode.
- The audit pipeline persisted up to ~3,000 rows/s into Postgres *and* Mongo with consumer
  lag ≤ 32 records and nothing dropped.
- 10,000 RPS of gateway overhead therefore needs roughly 20–25 replicas of this size (plus
  the concurrency headroom from "Scaling"), which is why the api tier scales horizontally.

### Per-container cost (realistic run, 1,000 RPS stage included)

| Container | CPU median | CPU p95 | Memory |
|---|---|---|---|
| api (each of 6) | 27–35% | 64–79% | ~130 MiB |
| audit-consumer (each of 2) | ~28% | ~42% | ~52 MiB |
| kafka | 27% | 133% | ~880 MiB |
| postgres | 24% | 37% | ~256 MiB |
| mongo | 15% | 59% | ~484 MiB |
| nginx | 8% | 22% | ~400 MiB |
| redis | 8% | 21% | ~8 MiB |

### Fault injection (all measured on the running stack)

| Fault, under load | What callers saw | What the system did |
|---|---|---|
| Provider B returns 503 for every request | 99.3% success (2,385 / 2,401) | B's breaker opened in ~6 s on every replica; one probe in half-open; closed again ~6 s after recovery |
| Redis paused for 13 s (connections hang) | 99% success, same as baseline; requests with `Idempotency-Key` got 503 | all replicas degraded to local limits within ~100 ms and recovered automatically |
| Kafka stopped for ~20 s | 98.6% success, same as baseline | 1,401 responses → 1,401 audit rows, no duplicates: failed deliveries were re-queued, nothing dropped |

### What the benchmark changed in the design

The first realistic run failed badly at 500 RPS (68% errors, p99 15 s). Profiling with
py-spy showed **62% of CPU in httpcore's `AsyncConnectionPool._assign_requests_to_connections`**:
httpx scans every pooled connection on each request and release, and slow providers keep
thousands of sockets open. The saturated event loop then made healthy providers look slow,
their breakers opened, and every request failed: a self-inflicted cascade. Fixes:

1. Upstream client moved to **aiohttp** behind a `Transport` interface (O(1) pool acquire).
2. **Event-loop-lag shedding**: CPU starvation now produces fast 503s, not timeouts.
3. **Timeouts that happen while our loop is lagging, or that the caller's deadline cut
   short, no longer count against a provider's breaker.**
4. redis-py's pipelines issued a `SCRIPT EXISTS` round-trip before every pipeline that
   carried a Lua script; scripts now run as plain `EVALSHA` and load only on `NOSCRIPT`.
5. The Kafka-outage test found audit events lost when an outage outlived aiokafka's
   internal retries: failed deliveries are now re-queued into the bounded buffer.

---

## Changes from the original plan

| Plan | Built | Why |
|---|---|---|
| httpx upstream client | aiohttp | measured: httpx pool scan was 62% of CPU at 400 RPS |
| accept caller `X-Request-ID` as the trace id | always mint a ULID `trace_id`; keep the caller's id as `client_request_id` and echo it | caller ids can collide, and the audit store dedupes on `trace_id`; ULIDs also carry the timestamp used to prune partitions on lookup |
| retry budget in a Redis rolling window | per replica | balanced replicas see representative traffic; saves a Redis call per request |
| `/readyz` fails when Redis or Kafka is down | reports them, fails only while starting or draining | failing readiness on audit-path dependencies would let them take down serving |
| load shedding on in-flight count | in-flight **and** event-loop lag | in-flight count cannot see CPU starvation on a slow-upstream proxy |
| `UNLOGGED` staging table | session `TEMP` table (`ON COMMIT DELETE ROWS`) | per-connection, so concurrent consumers never mix batches; same COPY speed |
