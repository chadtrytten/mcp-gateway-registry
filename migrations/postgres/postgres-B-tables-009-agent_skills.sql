-- ============================================================================
-- POSTGRES-B-009 — agent_skills
-- ABC: SkillRepositoryBase (interfaces.py:1109-1237)
-- Mongo: agent_skills_{namespace}    Pg: agent_skills_default
--
-- Purpose: SkillCard registry. Hot fields support list_by_owner /
-- list_by_visibility / list_by_registry_name + tag-based filtering.
-- The `tags TEXT[]` column uses native Postgres arrays (not JSONB) for
-- GIN-indexed contains/overlap queries via && and @> operators.
--
-- Authority: P3 §2.9 (template; detailed here per upstream
--            documentdb/skill_repository.py:101-119)
-- ============================================================================

CREATE TABLE IF NOT EXISTS agent_skills_default (
    id              TEXT PRIMARY KEY,
                    -- Skill path, e.g. '/skills/research-bot/summarize'

    name            TEXT GENERATED ALWAYS AS (data->>'name')          STORED,
    visibility      TEXT GENERATED ALWAYS AS (data->>'visibility')    STORED,
                    -- 'public' | 'private' | 'restricted'
    registry_name   TEXT GENERATED ALWAYS AS (data->>'registry_name') STORED,
                    -- Source registry (for federated skills)
    owner           TEXT GENERATED ALWAYS AS (data->>'owner')         STORED,
    is_enabled      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Tags as a native text[] for GIN-indexed && / @> queries (faster than
    -- JSONB array containment for this access pattern):
    tags            TEXT[] NOT NULL DEFAULT '{}'::text[],

    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agent_skills_default_data_gin
    ON agent_skills_default USING GIN (data jsonb_path_ops);

CREATE INDEX IF NOT EXISTS agent_skills_default_enabled
    ON agent_skills_default (is_enabled);

CREATE INDEX IF NOT EXISTS agent_skills_default_visibility
    ON agent_skills_default (visibility);

CREATE INDEX IF NOT EXISTS agent_skills_default_registry_name
    ON agent_skills_default (registry_name) WHERE registry_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS agent_skills_default_owner
    ON agent_skills_default (owner) WHERE owner IS NOT NULL;

-- Tag overlap queries: WHERE tags && ARRAY['llm','rag']::text[]
CREATE INDEX IF NOT EXISTS agent_skills_default_tags_gin
    ON agent_skills_default USING GIN (tags);

-- delete_with_versions parity:
CREATE INDEX IF NOT EXISTS agent_skills_default_id_prefix
    ON agent_skills_default (id text_pattern_ops);

CREATE TRIGGER agent_skills_default_updated_at
    BEFORE UPDATE ON agent_skills_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();

-- ----------------------------------------------------------------------------
-- IMPORTANT: SkillRepositoryBase requires `tags` to be writable independently
-- of `data` (some callers update tags via update_field('tags', [...])).
-- The repository write path keeps `tags` and `data->'tags'` in sync inside
-- a single UPDATE — do NOT make `tags` GENERATED, otherwise update_field
-- on the array fails (Pg refuses to update generated columns).
-- ----------------------------------------------------------------------------

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-009-agent_skills')
ON CONFLICT (name) DO NOTHING;
