-- ============================================================================
-- POSTGRES-B-001 — mcp_servers
-- ABC: ServerRepositoryBase (interfaces.py:26-181)
-- Mongo: mcp_servers_{namespace}    Pg: mcp_servers_default
--
-- Purpose: MCP server registry. One row per server path; versioned paths
-- (e.g. '/foo:v2') stored as separate rows but DELETE WITH VERSIONS uses
-- LIKE '/foo:%' (text_pattern_ops index supports prefix scan).
--
-- Authority: P3 §2.2
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_servers_default (
    id          TEXT PRIMARY KEY,
                -- e.g. '/context7' or '/context7:v2' (Mongo _id)

    server_name TEXT GENERATED ALWAYS AS (data->>'server_name')             STORED,
    source      TEXT GENERATED ALWAYS AS (data->>'source')                  STORED,
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
                -- Materialized (not generated): set_state() updates both
                -- is_enabled and data->>'is_enabled' atomically. Generated
                -- column would force a JSONB rewrite on every state flip.
    status      TEXT GENERATED ALWAYS AS (COALESCE(data->>'status','active')) STORED,

    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Containment / find_with_filter on arbitrary JSONB fields:
CREATE INDEX IF NOT EXISTS mcp_servers_default_data_gin
    ON mcp_servers_default USING GIN (data jsonb_path_ops);

-- list_by_source(source):
CREATE INDEX IF NOT EXISTS mcp_servers_default_source_btree
    ON mcp_servers_default (source) WHERE source IS NOT NULL;

-- "list enabled servers" + admin filters:
CREATE INDEX IF NOT EXISTS mcp_servers_default_enabled_btree
    ON mcp_servers_default (is_enabled);

-- Status filter (active/draft/deprecated):
CREATE INDEX IF NOT EXISTS mcp_servers_default_status_btree
    ON mcp_servers_default (status);

-- delete_with_versions: WHERE id LIKE '/foo:%' — needs text_pattern_ops:
CREATE INDEX IF NOT EXISTS mcp_servers_default_id_prefix
    ON mcp_servers_default (id text_pattern_ops);

CREATE TRIGGER mcp_servers_default_updated_at
    BEFORE UPDATE ON mcp_servers_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-001-servers')
ON CONFLICT (name) DO NOTHING;
