-- ============================================================================
-- POSTGRES-B-003 — mcp_scopes
-- ABC: ScopeRepositoryBase (interfaces.py:307-670, 16 abstract methods)
-- Mongo: mcp_scopes_{namespace}    Pg: mcp_scopes_default
--
-- Purpose: Authorization scopes + Keycloak group mappings + per-server
-- access control lists. Three first-class JSONB array/object fields are
-- broken out from `data` to support atomic ops via jsonb_set + jsonb_agg
-- (see P3 §3.4) and to allow direct GIN containment indexes.
--
-- Atomic JSONB ops use mcp_jsonb_array_add_unique() and
-- mcp_jsonb_array_remove_value() helpers from the migration prelude.
--
-- Authority: P3 §2.4 + §3.4
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_scopes_default (
    id              TEXT PRIMARY KEY,                       -- scope name (e.g. 'admin', 'mcp-user')

    ui_permissions  JSONB NOT NULL DEFAULT '{}'::jsonb,
                    -- shape: {"list_service": [server_path, ...],
                    --          "execute_tool": [server_path, ...], ...}
    server_access   JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- shape: [{"server": path, "methods": [...], "tools": [...]}, ...]
    group_mappings  JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- array of Keycloak group names (e.g. ['/admins', '/operators'])
    description     TEXT,

    data            JSONB NOT NULL,
                    -- Full Pydantic dump (also includes the three above fields,
                    -- kept in sync. The repo write-path uses jsonb_set on `data`
                    -- AND on the broken-out columns in a single UPDATE.)

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- get_group_mappings(keycloak_group): WHERE group_mappings @> to_jsonb($1::text)
CREATE INDEX IF NOT EXISTS mcp_scopes_default_groups_gin
    ON mcp_scopes_default USING GIN (group_mappings jsonb_path_ops);

-- list_scopes_for_server(server_path): WHERE server_access @> '[{"server": "..."}]'
CREATE INDEX IF NOT EXISTS mcp_scopes_default_server_access_gin
    ON mcp_scopes_default USING GIN (server_access jsonb_path_ops);

-- Generic find_with_filter:
CREATE INDEX IF NOT EXISTS mcp_scopes_default_data_gin
    ON mcp_scopes_default USING GIN (data jsonb_path_ops);

CREATE TRIGGER mcp_scopes_default_updated_at
    BEFORE UPDATE ON mcp_scopes_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

-- ----------------------------------------------------------------------------
-- Reference SQL for the 16 ScopeRepositoryBase methods (also see P3 §3.4):
--
-- get_group_mappings(group):
--   SELECT id FROM mcp_scopes_default
--   WHERE group_mappings @> to_jsonb($1::text);
--
-- add_server_to_ui_scopes(scope, perm, server):
--   UPDATE mcp_scopes_default
--   SET ui_permissions = jsonb_set(
--           ui_permissions, ARRAY[$2],
--           mcp_jsonb_array_add_unique(ui_permissions->$2, to_jsonb($3::text))
--       )
--   WHERE id = $1;
--
-- remove_server_from_ui_scopes(scope, perm, server):
--   UPDATE mcp_scopes_default
--   SET ui_permissions = jsonb_set(
--           ui_permissions, ARRAY[$2],
--           mcp_jsonb_array_remove_value(ui_permissions->$2, to_jsonb($3::text))
--       )
--   WHERE id = $1;
--
-- See P3 §3.4 for upsert-server-scope (server_access array entry replace/append).
-- ----------------------------------------------------------------------------

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-003-scopes')
ON CONFLICT (name) DO NOTHING;
