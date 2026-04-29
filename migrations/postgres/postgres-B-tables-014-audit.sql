-- ============================================================================
-- POSTGRES-B-014 — audit_events
-- ABC: AuditRepositoryBase (audit_repository.py:26-138)
-- Mongo: audit_events_{namespace}    Pg: audit_events_default
--
-- Purpose: Append-mostly audit log. Every privileged operation writes one
-- row. Read pattern: range queries on `timestamp` (DESC) with optional
-- filters on event_type / identity_username / target_path.
--
-- KNOWN LIMITATION: AuditRepositoryBase.aggregate(pipeline) takes a Mongo
-- aggregation pipeline; Postgres has no equivalent. The repository
-- implementation must (a) translate the small subset of pipeline stages
-- the UI actually uses, or (b) coordinate with upstream to add a
-- typed-params query method (see P3 §11 risk #5). The DDL here doesn't
-- block on that — pipeline translation is a runtime concern.
--
-- Partitioning: skip for v1. If row count exceeds ~10M/month, switch to
-- monthly RANGE partitions on `timestamp` and detach old partitions to
-- cheap storage (commented sketch at the bottom).
--
-- Authority: P3 §2.11
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_events_default (
    id                BIGSERIAL PRIMARY KEY,
                      -- Append-only surrogate; no UPSERT semantics.

    event_type        TEXT NOT NULL,
                      -- 'auth.login' | 'server.create' | 'scope.update' | ...

    timestamp         TIMESTAMPTZ NOT NULL,
                      -- Application supplies — distinct from created_at, which
                      -- is when the row landed (may differ for buffered writes).

    identity_username TEXT GENERATED ALWAYS AS (data #>> '{identity,username}') STORED,
                      -- nested JSONB path → flat indexable column

    target_path       TEXT GENERATED ALWAYS AS (data->>'target_path') STORED,

    data              JSONB NOT NULL,
                      -- {identity: {username, sub, groups: [...]},
                      --  target_path, action, before, after,
                      --  request_id, source_ip, user_agent, ...}

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Range queries by time (DESC for "latest first"):
CREATE INDEX IF NOT EXISTS audit_events_default_ts
    ON audit_events_default (timestamp DESC);

-- Filter by event class:
CREATE INDEX IF NOT EXISTS audit_events_default_event_type
    ON audit_events_default (event_type);

-- "Show me everything user X did" — partial index ignores rows lacking identity:
CREATE INDEX IF NOT EXISTS audit_events_default_username
    ON audit_events_default (identity_username)
    WHERE identity_username IS NOT NULL;

-- Filter by target (e.g. all events touching '/servers/foo'):
CREATE INDEX IF NOT EXISTS audit_events_default_target
    ON audit_events_default (target_path)
    WHERE target_path IS NOT NULL;

-- Containment / distinct() queries on arbitrary JSONB shape:
CREATE INDEX IF NOT EXISTS audit_events_default_data_gin
    ON audit_events_default USING GIN (data jsonb_path_ops);

-- ----------------------------------------------------------------------------
-- FUTURE: monthly partitioning sketch (skip for v1)
-- ----------------------------------------------------------------------------
-- When row count exceeds ~10M/month or retention requires cheap detachment:
--
-- CREATE TABLE audit_events_default (
--     ... (same columns) ...
-- ) PARTITION BY RANGE (timestamp);
--
-- CREATE TABLE audit_events_default_2026_05
--   PARTITION OF audit_events_default
--   FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
--
-- A pg_cron job (or migration script) creates next month's partition on
-- the 25th of each month. Old partitions can be DETACHed and ALTER TABLE
-- ... SET TABLESPACE 'archive' for cold storage.
--
-- Switching to partitioned later is a one-time migration — copy rows into
-- the partitioned shell, swap names, drop the old. ~5 minutes downtime if
-- done online with logical replication.
-- ----------------------------------------------------------------------------

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-014-audit')
ON CONFLICT (name) DO NOTHING;
