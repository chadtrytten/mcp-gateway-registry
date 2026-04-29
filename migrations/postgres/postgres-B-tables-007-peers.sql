-- ============================================================================
-- POSTGRES-B-007 — mcp_peers
-- ABC: PeerFederationRepositoryBase (interfaces.py:981-1051, peers half)
-- Mongo: mcp_peers_{namespace}    Pg: mcp_peers_default
--
-- Purpose: Federation peer registry — one row per remote registry we sync
-- with. The encrypted federation_token lives inside `data` (already
-- encrypted by the app layer; we store ciphertext only).
--
-- Note: Order of creation matters. mcp_peer_sync_state_default (§008) has
-- an FK back to this table — peers MUST be created before peer_sync_state.
--
-- Authority: P3 §2.8
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_peers_default (
    id          TEXT PRIMARY KEY,                       -- peer_id (UUID or human handle)
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
                -- Materialized so list-active-peers stays an index-only scan.
    data        JSONB NOT NULL,
                -- {endpoint_url, federation_token_ciphertext, public_key,
                --  shared_collections, sync_interval_seconds, ...}
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "list active peers" — the dominant read in the federation scheduler:
CREATE INDEX IF NOT EXISTS mcp_peers_default_enabled
    ON mcp_peers_default (enabled);

CREATE TRIGGER mcp_peers_default_updated_at
    BEFORE UPDATE ON mcp_peers_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-007-peers')
ON CONFLICT (name) DO NOTHING;
