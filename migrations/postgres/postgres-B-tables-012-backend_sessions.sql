-- ============================================================================
-- POSTGRES-B-012 — backend_sessions
-- ABC: BackendSessionRepositoryBase (interfaces.py:1239-1334)
-- Mongo: backend_sessions_{namespace}    Pg: backend_sessions_default
--
-- Purpose: Per-client backend MCP session bindings. Mongo uses a TTL index
-- on `last_used_at` with expireAfterSeconds=3600 (backend_session_repository.py:74-79).
-- Postgres has no native TTL → two equivalent strategies, both shown:
--
--   A. pg_cron job (recommended, RDS Postgres ≥ 15)
--   B. In-app asyncio sweeper (fallback when pg_cron is unavailable)
--
-- Choice is made at app boot via a probe:
--    SELECT 1 FROM pg_extension WHERE extname = 'pg_cron';
-- If present and the user has cron.schedule access → install Option A and skip
-- the sweeper. Otherwise → start the in-app sweeper task.
--
-- Authority: P3 §2.10
-- ============================================================================

CREATE TABLE IF NOT EXISTS backend_sessions_default (
    id                    TEXT PRIMARY KEY,
                          -- Composite key, one of:
                          --   'client:<client_session_id>'  (client-side row)
                          --   '<client_session_id>:<backend_key>'  (binding row)
    kind                  TEXT NOT NULL,
                          -- 'client' | 'backend'
    client_session_id     TEXT NOT NULL,
    backend_key           TEXT,
    backend_session_id    TEXT,
    user_id               TEXT,
    virtual_server_path   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Lookup-by-client (the dominant access pattern):
CREATE INDEX IF NOT EXISTS backend_sessions_default_client
    ON backend_sessions_default (client_session_id);

-- TTL sweeper scan (whichever strategy you choose below):
CREATE INDEX IF NOT EXISTS backend_sessions_default_last_used
    ON backend_sessions_default (last_used_at);

-- ----------------------------------------------------------------------------
-- OPTION A — pg_cron job (recommended)
-- ----------------------------------------------------------------------------
-- Requires the pg_cron extension to be installed AND the user running this
-- migration to have permission to call cron.schedule. Wrap in DO block so the
-- file remains idempotent and degrades gracefully if pg_cron is absent.
--
-- Default policy: every 5 minutes, delete rows with last_used_at older than
-- 1 hour (matches Mongo's expireAfterSeconds=3600 with the standard skew).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        -- Idempotent: cron.schedule errors if the named job already exists,
        -- so unschedule first.
        BEGIN
            PERFORM cron.unschedule('backend_sessions_default_ttl');
        EXCEPTION WHEN OTHERS THEN
            -- Job didn't exist; carry on.
            NULL;
        END;

        PERFORM cron.schedule(
            'backend_sessions_default_ttl',
            '*/5 * * * *',
            $cron$DELETE FROM backend_sessions_default WHERE last_used_at < now() - interval '1 hour'$cron$
        );
        RAISE NOTICE 'Scheduled pg_cron job: backend_sessions_default_ttl (every 5 min, TTL = 1 hour).';
    ELSE
        RAISE NOTICE 'pg_cron unavailable. Application MUST start the in-app sweeper task. See Option B comment in this file.';
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- OPTION B — in-app sweeper (fallback)
-- ----------------------------------------------------------------------------
-- If pg_cron is unavailable, the application launches an asyncio task at
-- startup that runs the equivalent DELETE every 5 minutes. Sketch:
--
--   async def _backend_sessions_sweeper():
--       while True:
--           try:
--               async with (await get_pool()).acquire() as conn:
--                   await conn.execute(
--                       "DELETE FROM backend_sessions_default "
--                       "WHERE last_used_at < now() - interval '1 hour'"
--                   )
--           except Exception:
--               logger.exception("backend_sessions sweeper failed")
--           await asyncio.sleep(300)
--
-- Pattern is already used in upstream (peer_sync_scheduler.py).
-- ----------------------------------------------------------------------------

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-012-backend_sessions')
ON CONFLICT (name) DO NOTHING;
