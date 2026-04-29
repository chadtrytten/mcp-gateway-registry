-- ============================================================================
-- POSTGRES-B-013 — registry_cards
-- ABC: RegistryCardRepositoryBase (interfaces.py:1460-1480)
-- Mongo: registry_cards_{namespace}    Pg: registry_cards_default
--
-- Purpose: Singleton — describes this registry's public-facing identity
-- (display name, owner contact, capabilities, advertised endpoints).
-- Always one row with id = 'default'. Kept as a table (not a typed
-- singleton) for symmetry with mcp_federation_config and Mongo collection
-- semantics.
--
-- Authority: P3 §2.9 (template)
-- ============================================================================

CREATE TABLE IF NOT EXISTS registry_cards_default (
    id          TEXT PRIMARY KEY,                       -- always 'default'
    data        JSONB NOT NULL,
                -- {registry_name, owner, contact, capabilities,
                --  advertised_endpoints, public_key, ...}
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Singleton: PK is the only access path. No secondary indexes.

CREATE TRIGGER registry_cards_default_updated_at
    BEFORE UPDATE ON registry_cards_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-013-registry_cards')
ON CONFLICT (name) DO NOTHING;
