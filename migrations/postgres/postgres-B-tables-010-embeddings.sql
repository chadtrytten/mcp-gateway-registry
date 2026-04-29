-- ============================================================================
-- POSTGRES-B-010 — mcp_embeddings_{N}
-- ABC: SearchRepositoryBase (interfaces.py:856-979) — the centerpiece.
-- Mongo: mcp_embeddings_{N}_{namespace}    Pg: mcp_embeddings_{N}_default
--
-- Purpose: Hybrid semantic + lexical search across all entity types
-- (mcp_server, mcp_agent, agent_skill, virtual_server). One row per
-- indexed entity. Vector column is nullable — when the embedding model is
-- unavailable, indexing succeeds and search degrades to lexical-only.
--
-- Dimension N is dynamic (settings.embeddings_model_dimensions):
--   384  — sentence-transformers/all-MiniLM-L6-v2
--   1024 — bedrock cohere
--   1536 — OpenAI text-embedding-3-small / Bedrock Titan v2 (default)
--   3072 — OpenAI text-embedding-3-large
--
-- Application creates the per-dimension table at startup via
-- CREATE TABLE IF NOT EXISTS using this template (P3 §3.5 initialize()).
-- This file ships the 1536 instance as the canonical example. To support
-- another dimension, copy this file, replace 1536 → <N> globally, and run.
--
-- HNSW parameters (m=16, ef_construction=128) match DocumentDB defaults
-- exactly so cross-backend recall parity is maintained
-- (search_repository.py:535-537). Per-query ef_search via:
--    SET LOCAL hnsw.ef_search = <settings.vector_search_ef_search>;  -- default 100
--
-- Authority: P3 §2.5 + §5
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_embeddings_1536_default (
    id                  TEXT PRIMARY KEY,
                        -- Entity path; one row per indexed entity.

    entity_type         TEXT NOT NULL,
                        -- 'mcp_server' | 'mcp_agent' | 'agent_skill' | 'virtual_server'
    name                TEXT,
    description         TEXT,

    tags                TEXT[] NOT NULL DEFAULT '{}'::text[],
                        -- Native array for GIN-indexed && / @> filters.
    metadata_text       TEXT,
                        -- Flattened metadata for keyword scan (lexical fallback).
    is_enabled          BOOLEAN NOT NULL DEFAULT FALSE,
    status              TEXT NOT NULL DEFAULT 'active',
                        -- 'active' | 'draft' | 'deprecated'

    text_for_embedding  TEXT,
                        -- The exact string used to generate `embedding`. Stored
                        -- so we can re-embed on model upgrade without re-deriving.

    embedding           vector(1536),
                        -- Nullable: lexical-only mode when the embedding model
                        -- isn't loaded. Search code checks `IS NOT NULL`.

    embedding_metadata  JSONB,
                        -- {model_name, model_version, dim, normalized: bool, ...}
    tools               JSONB NOT NULL DEFAULT '[]'::jsonb,
                        -- Tool definitions for mcp_server entities (passes
                        -- through into search results).
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
                        -- Original entity metadata snapshot.

    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Generated weighted tsvector for the lexical-only fallback path
    -- (search_repository.py:_lexical_only_search) and for the hybrid score
    -- boost. Weights mirror common keyword-search defaults: A=name, B=desc/tags,
    -- C=metadata. Single STORED column avoids re-computing on every search.
    text_tsv            tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(name, '')),                    'A')
     || setweight(to_tsvector('english', coalesce(description, '')),             'B')
     || setweight(to_tsvector('english', array_to_string(tags, ' ')),            'B')
     || setweight(to_tsvector('english', coalesce(metadata_text, '')),           'C')
    ) STORED
);

-- HNSW vector index — cosine similarity to match DocumentDB.
-- Parameters from search_repository.py:535-537. NOTE: HNSW build is O(N*log N)
-- and holds an exclusive lock on the table — use CONCURRENTLY in production
-- backfills (`CREATE INDEX CONCURRENTLY ...`). For initial empty-table create,
-- the lock is irrelevant and the standard form here is fine.
CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_hnsw
    ON mcp_embeddings_1536_default
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 128);

-- Lexical fallback / hybrid keyword boost:
CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_tsv
    ON mcp_embeddings_1536_default USING GIN (text_tsv);

-- Tag overlap filter (entity-tag intersection):
CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_tags_gin
    ON mcp_embeddings_1536_default USING GIN (tags);

-- Standard scalar filters used in the WHERE clause of vector candidate selection:
CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_entity_type
    ON mcp_embeddings_1536_default (entity_type);

CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_enabled
    ON mcp_embeddings_1536_default (is_enabled);

CREATE INDEX IF NOT EXISTS mcp_embeddings_1536_default_status
    ON mcp_embeddings_1536_default (status);

-- ----------------------------------------------------------------------------
-- Reference query — hybrid (vector + lexical) search.
-- Mirrors documentdb $search.vectorSearch + lexical boost (search_repository.py:1661-1729).
--
-- BEGIN;
--   SET LOCAL hnsw.ef_search = 100;  -- from settings.vector_search_ef_search
--
--   WITH vec AS (
--       SELECT id, entity_type, name, description, tags, status, is_enabled,
--              tools, metadata, indexed_at,
--              1 - (embedding <=> $1::vector) AS vector_score
--       FROM mcp_embeddings_1536_default
--       WHERE embedding IS NOT NULL
--         AND entity_type = ANY($2::text[])
--         AND is_enabled = TRUE
--       ORDER BY embedding <=> $1::vector
--       LIMIT $3                       -- = max(max_results * 3, 50)
--   )
--   SELECT v.*,
--          ts_rank_cd(f.text_tsv, plainto_tsquery('english', $4)) AS lexical_rank,
--          GREATEST(0.0, LEAST(1.0,
--              (vector_score + 1.0) / 2.0
--            + ts_rank_cd(f.text_tsv, plainto_tsquery('english', $4)) * 0.1
--          )) AS final_score
--   FROM vec v
--   JOIN mcp_embeddings_1536_default f USING (id)
--   ORDER BY final_score DESC
--   LIMIT $5;                          -- = max_results
-- COMMIT;
-- ----------------------------------------------------------------------------

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-010-embeddings-1536')
ON CONFLICT (name) DO NOTHING;
