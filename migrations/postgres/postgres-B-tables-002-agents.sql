-- ============================================================================
-- POSTGRES-B-002 — mcp_agents
-- ABC: AgentRepositoryBase (interfaces.py:183-305)
-- Mongo: mcp_agents_{namespace}    Pg: mcp_agents_default
--
-- Purpose: A2A AgentCard registry. Same shape as servers (id, name,
-- visibility, is_enabled). Used by find_with_filter for ANS queries
-- ({"ans_metadata": {"$exists": True, "$ne": None}}); §2.4 GIN index covers it.
--
-- Authority: P3 §2.3
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_agents_default (
    id          TEXT PRIMARY KEY,
                -- AgentCard path, e.g. '/agents/research-bot'

    name        TEXT GENERATED ALWAYS AS (data->>'name')        STORED,
    visibility  TEXT GENERATED ALWAYS AS (data->>'visibility')  STORED,
                -- 'public' | 'private' | 'restricted'
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
                -- Materialized; set_state() writes both columns atomically.

    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS mcp_agents_default_data_gin
    ON mcp_agents_default USING GIN (data jsonb_path_ops);

CREATE INDEX IF NOT EXISTS mcp_agents_default_enabled
    ON mcp_agents_default (is_enabled);

CREATE INDEX IF NOT EXISTS mcp_agents_default_visibility
    ON mcp_agents_default (visibility);

-- delete_with_versions parity:
CREATE INDEX IF NOT EXISTS mcp_agents_default_id_prefix
    ON mcp_agents_default (id text_pattern_ops);

CREATE TRIGGER mcp_agents_default_updated_at
    BEFORE UPDATE ON mcp_agents_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-002-agents')
ON CONFLICT (name) DO NOTHING;
