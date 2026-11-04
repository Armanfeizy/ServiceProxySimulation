-- Audit summary store: one narrow row (~200 bytes) per caller request.
-- Range-partitioned by day: retention is DROP TABLE (instant, no DELETE, no vacuum storm).

CREATE TABLE IF NOT EXISTS audit_request (
    received_at     timestamptz NOT NULL,
    trace_id        text        NOT NULL,   -- gateway-minted ULID
    client_request_id text,                 -- caller's X-Request-ID, if any
    tenant_id       text,
    idempotency_key text,
    provider        text,
    attempt_count   smallint,
    status          text,       -- ok | client_error | provider_error | rate_limited |
                                -- unavailable | deadline_exceeded | shed | throttled | ...
    http_code       smallint,
    total_ms        real,
    provider_ms     real,
    error_class     text,
    PRIMARY KEY (received_at, trace_id)
) PARTITION BY RANGE (received_at);

-- Catches rows outside every daily partition (clock skew, late replays) instead of
-- failing the batch. Kept small by creating partitions ahead of time.
CREATE TABLE IF NOT EXISTS audit_request_default PARTITION OF audit_request DEFAULT;

-- Pre-aggregated per-minute rollup, so dashboards never scan raw partitions.
CREATE TABLE IF NOT EXISTS audit_rollup_1m (
    bucket      timestamptz NOT NULL,
    provider    text        NOT NULL,
    status      text        NOT NULL,
    requests    bigint      NOT NULL,
    p50_ms      real,
    p95_ms      real,
    p99_ms      real,
    avg_ms      real,
    PRIMARY KEY (bucket, provider, status)
);

-- Idempotent: safe to call from Celery beat and from the consumer at startup.
CREATE OR REPLACE FUNCTION ensure_audit_partitions(days_ahead integer DEFAULT 3)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    d       date;
    name    text;
    created integer := 0;
BEGIN
    FOR i IN -1..days_ahead LOOP
        d := (now() AT TIME ZONE 'UTC')::date + i;
        name := format('audit_request_%s', to_char(d, 'YYYYMMDD'));
        IF to_regclass(name) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF audit_request FOR VALUES FROM (%L) TO (%L)',
                name, d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END $$;

CREATE OR REPLACE FUNCTION drop_expired_audit_partitions(retention_days integer)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    part    record;
    cutoff  date := (now() AT TIME ZONE 'UTC')::date - retention_days;
    dropped integer := 0;
BEGIN
    FOR part IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'audit_request' AND c.relname ~ '^audit_request_\d{8}$'
    LOOP
        IF to_date(right(part.relname, 8), 'YYYYMMDD') < cutoff THEN
            EXECUTE format('DROP TABLE %I', part.relname);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END $$;

SELECT ensure_audit_partitions(3);
