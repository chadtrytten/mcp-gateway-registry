-- ============================================================================
-- POSTGRES-B-008 — mcp_peer_sync_state
-- ABC: PeerFederationRepositoryBase (interfaces.py:981-1051, sync half)
-- Mongo: mcp_peer_sync_state_{namespace}    Pg: mcp_peer_sync_state_default
--
-- Purpose: Per-peer sync watermarks/state. 1:1 with mcp_peers via FK.
-- ON DELETE CASCADE replaces the app-level cleanup pattern in
-- delete_peer() — Postgres FK gives atomic guarantee; the documentdb
-- impl has to cleanup in two operations.
--
-- DEPENDS ON: postgres-B-tables-007-peers.sql (must run first)
--
-- Authority: P3 §2.8
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_peer_sync_state_default (
    id          TEXT PRIMARY KEY
                    REFERENCES mcp_peers_default(id) ON DELETE CASCADE,
                -- Same peer_id as mcp_peers_default.id; FK enforces the 1:1.

    data        JSONB NOT NULL,
                -- {last_sync_at, last_sync_status, last_error,
                --  cursor_per_collection: {mcp_servers: "...", mcp_agents: "..."}, ...}

    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- No created_at: sync state is updated, not appended; only the latest
-- watermark matters.

CREATE TRIGGER mcp_peer_sync_state_default_updated_at
    BEFORE UPDATE ON mcp_peer_sync_state_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-008-peer_sync_state')
ON CONFLICT (name) DO NOTHING;
