-- ============================================================================
-- POSTGRES-B-005 — mcp_skill_security_scans
-- ABC: SkillSecurityScanRepositoryBase (interfaces.py:764-854)
-- Mongo: mcp_skill_security_scans_{namespace}    Pg: mcp_skill_security_scans_default
--
-- Purpose: One-to-many — same shape as mcp_security_scans (§004) except
-- the scan target is a skill_path instead of a server_path. Kept as a
-- separate table (not a discriminator column) to mirror upstream's two
-- distinct ABCs and to keep indexes tight.
--
-- Authority: P3 §2.6 (final paragraph)
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_skill_security_scans_default (
    id                      BIGSERIAL PRIMARY KEY,

    skill_path              TEXT NOT NULL,
    scan_status             TEXT NOT NULL,
    scan_timestamp          TIMESTAMPTZ NOT NULL DEFAULT now(),

    total_vulnerabilities   INTEGER NOT NULL DEFAULT 0,
    critical_count          INTEGER NOT NULL DEFAULT 0,
    high_count              INTEGER NOT NULL DEFAULT 0,
    medium_count            INTEGER NOT NULL DEFAULT 0,
    low_count               INTEGER NOT NULL DEFAULT 0,

    data                    JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS mcp_skill_security_scans_default_skill_ts
    ON mcp_skill_security_scans_default (skill_path, scan_timestamp DESC);

CREATE INDEX IF NOT EXISTS mcp_skill_security_scans_default_status
    ON mcp_skill_security_scans_default (scan_status);

CREATE INDEX IF NOT EXISTS mcp_skill_security_scans_default_data_gin
    ON mcp_skill_security_scans_default USING GIN (data jsonb_path_ops);

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-005-skill_security_scans')
ON CONFLICT (name) DO NOTHING;
