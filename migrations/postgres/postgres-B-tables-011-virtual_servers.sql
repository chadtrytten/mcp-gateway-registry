-- ============================================================================
-- POSTGRES-B-011 — virtual_servers
-- ABC: VirtualServerRepositoryBase (interfaces.py:1336-1458)
-- Mongo: virtual_servers_{namespace}    Pg: virtual_servers_default
--
-- Purpose: Virtual MCP server configurations — a "compose" layer that
-- aggregates multiple physical servers into one virtual endpoint. Hot
-- columns mirror upstream documentdb/virtual_server_repository.py:79-95.
--
-- Authority: P3 §2.9 (template; detailed here)
-- ============================================================================

CREATE TABLE IF NOT EXISTS virtual_servers_default (
    id          TEXT PRIMARY KEY,
                -- Virtual server path, e.g. '/virtual/research-suite'

    server_name TEXT GENERATED ALWAYS AS (data->>'server_name') STORED,
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,

    -- Tags as native text[] (same pattern as agent_skills, embeddings):
    tags        TEXT[] NOT NULL DEFAULT '{}'::text[],

    data        JSONB NOT NULL,
                -- {server_name, description, owner, member_servers: [...],
                --  tools: [...], scopes: [...], ...}

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS virtual_servers_default_data_gin
    ON virtual_servers_default USING GIN (data jsonb_path_ops);

CREATE INDEX IF NOT EXISTS virtual_servers_default_server_name
    ON virtual_servers_default (server_name) WHERE server_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS virtual_servers_default_enabled
    ON virtual_servers_default (is_enabled);

CREATE INDEX IF NOT EXISTS virtual_servers_default_tags_gin
    ON virtual_servers_default USING GIN (tags);

-- delete_with_versions parity:
CREATE INDEX IF NOT EXISTS virtual_servers_default_id_prefix
    ON virtual_servers_default (id text_pattern_ops);

CREATE TRIGGER virtual_servers_default_updated_at
    BEFORE UPDATE ON virtual_servers_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-011-virtual_servers')
ON CONFLICT (name) DO NOTHING;
