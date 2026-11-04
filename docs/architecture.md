# Architecture

> **Principle.** Redis is the only datastore on the request path. Everything else lives
> behind a queue, so no database can slow down or fail a caller.

## Components and data flow

```mermaid
flowchart LR
    client([Caller]) -->|POST /v1/process| nginx[Nginx<br/>least_conn, keep-alive]
    nginx --> api1[api replica 1]
    nginx --> api2[api replica …N]

    subgraph api[api replica — FastAPI, one event loop]
        direction TB
        mw[Middleware<br/>trace_id ULID · deadline clock · span] --> adm[Admission<br/>shed: in-flight + loop lag<br/>inbound limit · idempotency]
        adm --> sel[Selector<br/>P2C on EWMA × inflight<br/>breaker · bulkhead]
        sel --> disp[Dispatcher<br/>attempt · classify · retry budget]
        disp --> buf[(bounded audit buffer)]
    end

    api1 -.- api
    adm <-->|1 pipelined round-trip| redis[(Redis<br/>token buckets · breakers<br/>AIMD · idempotency)]
    sel <-->|1 Lua call / attempt| redis
    disp -->|aiohttp pools| A[Provider A<br/>300–1000ms, strict 429]
    disp --> B[Provider B<br/>1–10s, 5xx, timeouts]
    disp --> C[Provider C<br/>1–10s, 5xx, timeouts]

    buf -->|linger 50ms, lz4| kafka[(Kafka<br/>audit.requests ×12)]
    kafka --> cons[audit-consumer ×M]
    cons -->|COPY → INSERT ON CONFLICT| pg[(Postgres<br/>daily partitions)]
    cons -->|insert_many unordered| mongo[(MongoDB<br/>payloads, TTL)]
    cons -->|poison| dlq[(audit.requests.dlq)]

    beat[celery-beat] --> rmq[(RabbitMQ)] --> worker[celery-worker]
    worker -->|partitions · retention · rollups| pg
    worker -->|replay| dlq

    api1 -->|/metrics| prom[Prometheus] --> graf[Grafana]
    cons -->|/metrics| prom
    api1 -->|OTLP| jaeger[Jaeger]
    cons -->|OTLP| jaeger
```

## Request lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant G as api replica
    participant R as Redis
    participant P as Provider (A/B/C)
    participant K as Kafka
    participant W as audit-consumer
    C->>G: POST /v1/process
    Note over G: mint ULID trace_id, start 10s deadline
    G->>G: shed if inflight >= MAX_INFLIGHT or event loop lagging (503)
    G->>R: pipeline: inbound bucket + SET NX idem key + GET
    R-->>G: allowed? / replay / conflict
    G->>G: P2C over eligible providers (local cache)
    G->>R: outbound token bucket (Lua, global)
    G->>P: attempt, timeout = min(provider, deadline left)
    P-->>G: 2xx / 429 / 5xx / timeout
    alt failure that proves "not processed" and budget allows
        G->>P: retry on another provider
    end
    G->>R: SET idempotency result
    G-->>C: response (+ X-Trace-ID)
    G-)K: audit event (off the request path, batched)
    K-)W: batch of ~1000
    W-)W: Postgres COPY + Mongo bulk, then commit offsets
```

## Where state lives

| State | Scope | Store | Freshness |
|---|---|---|---|
| Outbound token bucket per provider | global | Redis (Lua, per attempt) | exact |
| Effective RPS (AIMD), 429 block window | global | Redis, cached locally | ≤ 200 ms |
| Circuit breaker state + 10×1s counts | global | Redis, cached locally | ≤ 200 ms |
| Half-open probe lock | global | Redis `SET NX PX` | exact |
| Inbound token bucket per API key | global | Redis (Lua, per request) | exact |
| Idempotency records | global | Redis, TTL 24h | exact |
| EWMA latency, in-flight, bulkhead | per replica | memory | exact |
| Retry budget | per replica | memory | exact |
| Load-shedding counter | per replica | memory | exact |
| Audit summary | durable | Postgres | ~0.1–1 s behind |
| Audit payload | durable | MongoDB | ~0.1–1 s behind |

## Failure modes

| Failure | Effect on callers | Mechanism |
|---|---|---|
| Provider slow / erroring | Traffic moves to healthier providers | EWMA in P2C, circuit breaker, bulkhead |
| Provider rate-limits (429) | Spill-over to others; A's real limit is learned | AIMD + Retry-After block window |
| Upstream brownout | Retries capped at ~10% of traffic | Retry budget |
| Overload (too many in flight) | Fast 503 + Retry-After, no queueing past the deadline | In-flight cap at admission |
| CPU starvation (own load or a busy host) | Progressive 503s instead of everyone timing out; providers are not blamed | Event-loop-lag shedding; lag-time timeouts don't count against breakers |
| Redis down | Keeps serving on local per-replica limits; idempotent requests get 503 | Degrade-open |
| Kafka down | Keeps serving; audit events dropped and counted | Bounded buffer |
| Postgres / Mongo down | Keeps serving; consumer stops committing, catches up later | Kafka retention |
