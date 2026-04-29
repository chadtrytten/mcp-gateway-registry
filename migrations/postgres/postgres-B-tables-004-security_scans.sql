-- ============================================================================
-- POSTGRES-B-004 — mcp_security_scans
-- ABC: SecurityScanRepositoryBase (interfaces.py:672-762)
-- Mongo: mcp_security_scans_{namespace}    Pg: mcp_security_scans_default
--
-- Purpose: One-to-many — many scan results per server_path. get_latest()
-- is the dominant read path (compound index on (server_path, scan_timestamp DESC)
-- serves it in O(log n)). Severity counts are denormalized for fast
-- aggregation in dashboards.
--
-- Authority: P3 §2.6
-- ============================================================================

CREATE TABLE IF NOT EXISTS mcp_security_scans_default (
    id                      BIGSERIAL PRIMARY KEY,
                            -- Surrogate key: scans aren't addressed by path,
                            -- they're appended and queried by (server_path, ts).

    server_path             TEXT NOT NULL,
    scan_status             TEXT NOT NULL,
                            -- 'pending' | 'running' | 'completed' | 'failed'
    scan_timestamp          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Denormalized severity counts (computed at insert from data->'vulnerabilities'):
    total_vulnerabilities   INTEGER NOT NULL DEFAULT 0,
    critical_count          INTEGER NOT NULL DEFAULT 0,
    high_count              INTEGER NOT NULL DEFAULT 0,
    medium_count            INTEGER NOT NULL DEFAULT 0,
    low_count               INTEGER NOT NULL DEFAULT 0,

    data                    JSONB NOT NULL,
                            -- Full scan payload incl. vulnerabilities array,
                            -- scanner version, raw findings.

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- get_latest(server_path):
--   SELECT data FROM mcp_security_scans_default
--   WHERE server_path = $1 ORDER BY scan_timestamp DESC LIMIT 1;
CREATE INDEX IF NOT EXISTS mcp_security_scans_default_server_ts
    ON mcp_security_scans_default (server_path, scan_timestamp DESC);

-- list_by_status(status):
CREATE INDEX IF NOT EXISTS mcp_security_scans_default_status
    ON mcp_security_scans_default (scan_status);

-- Generic find_with_filter on vulnerability shape:
CREATE INDEX IF NOT EXISTS mcp_security_scans_default_data_gin
    ON mcp_security_scans_default USING GIN (data jsonb_path_ops);

INSERT INTO mcp_migrations (name) VALUES ('postgres-B-tables-004-security_scans')
ON CONFLICT (name) DO NOTHING;
