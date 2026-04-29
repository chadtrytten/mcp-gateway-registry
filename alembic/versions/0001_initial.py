"""initial — load prelude + 14 table SQL files.

Revision ID: 0001_initial
Revises:
Create Date: 2026-04-29

The canonical schema lives in migrations/postgres/ as hand-curated SQL
(POSTGRES-B Phase A output). This revision is a thin runner that applies
those files in order under alembic's version tracking.

Order:
  000-prelude.sql                                — extensions + helpers
  postgres-B-tables-001-servers.sql              — mcp_servers
  postgres-B-tables-002-agents.sql               — mcp_agents
  postgres-B-tables-003-scopes.sql               — mcp_scopes
  postgres-B-tables-004-security_scans.sql
  postgres-B-tables-005-skill_security_scans.sql
  postgres-B-tables-006-federation_config.sql
  postgres-B-tables-007-peers.sql
  postgres-B-tables-008-peer_sync_state.sql
  postgres-B-tables-009-agent_skills.sql
  postgres-B-tables-010-embeddings.sql           — pgvector(N) HNSW
  postgres-B-tables-011-virtual_servers.sql
  postgres-B-tables-012-backend_sessions.sql
  postgres-B-tables-013-registry_cards.sql
  postgres-B-tables-014-audit.sql

Idempotent — every DDL file uses CREATE ... IF NOT EXISTS / OR REPLACE.

NOTE: 010-embeddings.sql is shipped at vector(1536). For other dimensions
override via the EMBEDDINGS_MODEL_DIMENSIONS env var; the application will
CREATE TABLE IF NOT EXISTS the per-dimension table at startup
(see docs/postgres-backend-design/C-pgvector-config.md §3.5).
"""

from __future__ import annotations

import pathlib

from alembic import op


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


_MIGRATIONS_DIR_CANDIDATES = (
    pathlib.Path("/app/migrations/postgres"),
    pathlib.Path(__file__).resolve().parents[2] / "migrations" / "postgres",
)

_FILES_IN_ORDER = (
    "000-prelude.sql",
    "postgres-B-tables-001-servers.sql",
    "postgres-B-tables-002-agents.sql",
    "postgres-B-tables-003-scopes.sql",
    "postgres-B-tables-004-security_scans.sql",
    "postgres-B-tables-005-skill_security_scans.sql",
    "postgres-B-tables-006-federation_config.sql",
    "postgres-B-tables-007-peers.sql",
    "postgres-B-tables-008-peer_sync_state.sql",
    "postgres-B-tables-009-agent_skills.sql",
    "postgres-B-tables-010-embeddings.sql",
    "postgres-B-tables-011-virtual_servers.sql",
    "postgres-B-tables-012-backend_sessions.sql",
    "postgres-B-tables-013-registry_cards.sql",
    "postgres-B-tables-014-audit.sql",
)


def _resolve_migrations_dir() -> pathlib.Path:
    for candidate in _MIGRATIONS_DIR_CANDIDATES:
        if candidate.is_dir():
            return candidate
    raise RuntimeError(
        "Could not locate migrations/postgres/. Tried: "
        + ", ".join(str(p) for p in _MIGRATIONS_DIR_CANDIDATES)
    )


def upgrade() -> None:
    base = _resolve_migrations_dir()
    for name in _FILES_IN_ORDER:
        path = base / name
        if not path.is_file():
            raise RuntimeError(f"Missing schema file: {path}")
        sql = path.read_text(encoding="utf-8")
        op.execute(sql)


def downgrade() -> None:
    # Schema teardown is destructive — never auto-down a registry. Operators
    # who need a clean slate should drop and recreate the database, then
    # `alembic stamp head`.
    raise NotImplementedError(
        "Downgrade not supported. To reset, drop the database and "
        "re-run `alembic upgrade head`."
    )
