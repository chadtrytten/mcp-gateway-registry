-- ============================================================================
-- POSTGRES-B-006 — mcp_federation_config
-- ABC: FederationConfigRepositoryBase (interfaces.py:1053-1107)
-- Mongo: mcp_federation_config_{namespace}    Pg: mcp_federation_config_default
--
-- Purpose: Singleton config for this registry's own federation identity
-- (registry_id, signing key, advertised endpoints). Effectively one row
-- with id = 'default'. Kept as a table (not a typed singleton) to mirror
-- Mongo collection semantics and to allow per-namespace config.
--
-- Authority: P3 §2.7
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_federation_config_default (
    id          TEXT PRIMARY KEY,                       -- typically 'default'
    data        JSONB NOT NULL,
                -- {registry_id, signing_pubkey, advertised_endpoints,
                --  trusted_issuers, ...}
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Singleton table: no secondary indexes. PK is the only access path.

CREATE TRIGGER mcp_federation_config_default_updated_at
    BEFORE UPDATE ON mcp_federation_config_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-006-federation_config')
ON CONFLICT (name) DO NOTHING;
