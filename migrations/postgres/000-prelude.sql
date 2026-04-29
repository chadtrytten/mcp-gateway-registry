-- ============================================================================
-- POSTGRES-B-000 — Migration prelude
-- Extensions, helper functions, application role
--
-- Target: PostgreSQL 16+
-- Order:  Run FIRST, before any postgres-B-tables-*.sql
-- Idempotent: yes (CREATE ... IF NOT EXISTS / OR REPLACE throughout)
--
-- Authority: P3 §2 (agent-P3-implementation-plan.md, batch-NEXT-20)
-- Author:    POSTGRES-B (CCLI2 batch-NEXT-27 phase-A)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- §1 Extensions
-- ----------------------------------------------------------------------------

-- pgcrypto: gen_random_uuid(), digest(), HMAC. Used for opaque IDs and
-- federation-token derivation. Available on every Postgres 16 build.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- pgvector: vector type + HNSW/IVFFlat indexes for embeddings table.
-- Required version: >= 0.7 (HNSW with WITH (m, ef_construction) syntax).
-- Image: pgvector/pgvector:pg16  |  RDS: Postgres >= 15.2  |  apt: postgresql-16-pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- pg_stat_statements: per-statement query stats. Required for §11 ops monitoring
-- (slow query detection on hybrid search). Must be in shared_preload_libraries
-- to function; CREATE EXTENSION succeeds either way.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- pg_cron: scheduled jobs in-database. Used by backend_sessions TTL sweeper
-- (§012). Not available on every managed Postgres (e.g. some Aurora releases,
-- some Cloud SQL configs). Conditional install — fall back to in-app sweeper
-- documented in 012-backend_sessions.sql if the extension is unavailable.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_cron;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pg_cron unavailable (%); backend_sessions TTL must use in-app sweeper. See 012-backend_sessions.sql.', SQLERRM;
END $$;

-- ----------------------------------------------------------------------------
-- §2 Helper functions
-- ----------------------------------------------------------------------------

-- mcp_set_updated_at: BEFORE UPDATE trigger function attached to every table
-- that has an updated_at column. Sets NEW.updated_at = now() unconditionally
-- (callers cannot override; mirrors Mongo server-side timestamp pattern).
CREATE OR REPLACE FUNCTION mcp_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

-- mcp_jsonb_array_remove_value: remove all occurrences of a scalar value from
-- a JSONB array. Used by mcp_scopes atomic ops (§003) for
-- remove_server_from_ui_scopes / remove_group_from_scope. Returns '[]'::jsonb
-- if input is NULL or empty.
CREATE OR REPLACE FUNCTION mcp_jsonb_array_remove_value(arr JSONB, val JSONB)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        (SELECT jsonb_agg(elem)
         FROM jsonb_array_elements(COALESCE(arr, '[]'::jsonb)) elem
         WHERE elem <> val),
        '[]'::jsonb
    );
$$;

-- mcp_jsonb_array_add_unique: append a scalar value to a JSONB array iff not
-- already present (set-semantics). Used by mcp_scopes for
-- add_server_to_ui_scopes / add_group_to_scope.
CREATE OR REPLACE FUNCTION mcp_jsonb_array_add_unique(arr JSONB, val JSONB)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN COALESCE(arr, '[]'::jsonb) @> jsonb_build_array(val)
            THEN COALESCE(arr, '[]'::jsonb)
        ELSE COALESCE(arr, '[]'::jsonb) || jsonb_build_array(val)
    END;
$$;

-- ----------------------------------------------------------------------------
-- §3 Migration tracking table
-- ----------------------------------------------------------------------------
-- Lets the in-app migration runner (P3 §5.2) know which DDL files have already
-- been applied. Advisory-lock the entire migration run so concurrent app boots
-- don't race.

CREATE TABLE IF NOT EXISTS mcp_migrations (
    name        TEXT PRIMARY KEY,           -- e.g. 'postgres-B-tables-001-servers'
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    checksum    TEXT                        -- sha256 of the SQL file (optional)
);

-- ----------------------------------------------------------------------------
-- §4 Application role
-- ----------------------------------------------------------------------------
-- The application connects as `mcp_registry`. Migrations should run as a
-- superuser or a role with CREATE on schema public + ability to install
-- extensions; the application role only needs DML + sequence + EXECUTE.
--
-- Password is set via env (do NOT commit a password literal). Comment-out the
-- CREATE ROLE for environments where the role is provisioned by infra (e.g.
-- terraform-managed RDS).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp_registry') THEN
        CREATE ROLE mcp_registry LOGIN;
        RAISE NOTICE 'Created role mcp_registry. Set password out-of-band: ALTER ROLE mcp_registry PASSWORD ''<from-env>'';';
    END IF;
END $$;

-- Schema usage + create-on-default. Adjust if you isolate the registry into a
-- non-public schema (recommended in shared clusters).
GRANT USAGE ON SCHEMA public TO mcp_registry;
GRANT CREATE ON SCHEMA public TO mcp_registry;  -- needed for dynamic embedding-table create

-- Tables created by THIS migration prelude:
GRANT SELECT, INSERT, UPDATE, DELETE ON mcp_migrations TO mcp_registry;

-- After all per-table DDL files are applied, also grant on those.
-- ALTER DEFAULT PRIVILEGES handles future tables created by the migration role.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO mcp_registry;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO mcp_registry;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO mcp_registry;

-- ----------------------------------------------------------------------------
-- §5 Record this migration
-- ----------------------------------------------------------------------------
INSERT INTO mcp_migrations (name) VALUES ('postgres-B-migration-prelude')
ON CONFLICT (name) DO NOTHING;
