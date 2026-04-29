"""Integration tests for PostgresSecurityScanRepository.

Skipped in CI when no DB is available; set ``POSTGRES_TEST_DSN`` to enable.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from registry.repositories.postgres.security_scan_repository import (
    PostgresSecurityScanRepository,
)

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason="Requires Postgres running — set POSTGRES_TEST_DSN to enable.",
    ),
]


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id                      BIGSERIAL PRIMARY KEY,
    server_path             TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS {idx}_server_ts
    ON {table} (server_path, scan_timestamp DESC);
CREATE INDEX IF NOT EXISTS {idx}_status ON {table} (scan_status);
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN
    schema = f"test_pgscan_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_security_scans_default'
    idx = f'"{schema}".mcp_security_scans_default'.replace('"', "").replace(".", "_")
    async with pool.acquire() as conn:
        await conn.execute(_TABLE_DDL.format(table=table, idx=idx))

    from registry.repositories.postgres import security_scan_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    repo = PostgresSecurityScanRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_scan(server_path: str = "/srv/foo", status: str = "completed") -> dict:
    return {
        "server_path": server_path,
        "scan_status": status,
        "scan_timestamp": datetime.now(timezone.utc).isoformat(),
        "vulnerabilities": [
            {"id": "CVE-1", "severity": "critical"},
            {"id": "CVE-2", "severity": "high"},
            {"id": "CVE-3", "severity": "low"},
        ],
        "scanner_version": "1.0.0",
    }


async def test_get_returns_none_when_missing(isolated_repo):
    assert await isolated_repo.get("/srv/none") is None


async def test_create_returns_true_and_indexes_counts(isolated_repo):
    assert await isolated_repo.create(_sample_scan()) is True
    fetched = await isolated_repo.get("/srv/foo")
    assert fetched is not None
    assert fetched["total_vulnerabilities"] == 3
    assert fetched["critical_count"] == 1
    assert fetched["high_count"] == 1
    assert fetched["low_count"] == 1


async def test_create_without_path_returns_false(isolated_repo):
    assert await isolated_repo.create({"scan_status": "completed"}) is False


async def test_create_accepts_agent_path(isolated_repo):
    assert await isolated_repo.create({
        "agent_path": "/agents/x",
        "scan_status": "completed",
        "vulnerabilities": [],
    }) is True
    fetched = await isolated_repo.get("/agents/x")
    assert fetched is not None


async def test_get_latest_returns_newest(isolated_repo):
    first = _sample_scan()
    first["scan_timestamp"] = "2026-01-01T00:00:00+00:00"
    first["scanner_version"] = "old"
    await isolated_repo.create(first)
    second = _sample_scan()
    second["scan_timestamp"] = "2026-04-01T00:00:00+00:00"
    second["scanner_version"] = "new"
    await isolated_repo.create(second)
    latest = await isolated_repo.get_latest("/srv/foo")
    assert latest is not None
    assert latest["scanner_version"] == "new"


async def test_get_latest_handles_trailing_slash_variants(isolated_repo):
    # Stored without slash, queried with slash.
    await isolated_repo.create(_sample_scan(server_path="/srv/bar"))
    assert await isolated_repo.get_latest("/srv/bar/") is not None
    # Stored with slash, queried without.
    await isolated_repo.create(_sample_scan(server_path="/srv/baz/"))
    assert await isolated_repo.get_latest("/srv/baz") is not None


async def test_list_all_sorted_desc(isolated_repo):
    older = _sample_scan(server_path="/srv/x")
    older["scan_timestamp"] = "2026-01-01T00:00:00+00:00"
    older["scanner_version"] = "old"
    await isolated_repo.create(older)
    newer = _sample_scan(server_path="/srv/y")
    newer["scan_timestamp"] = "2026-04-01T00:00:00+00:00"
    newer["scanner_version"] = "new"
    await isolated_repo.create(newer)
    rows = await isolated_repo.list_all()
    assert len(rows) == 2
    assert rows[0]["scanner_version"] == "new"


async def test_query_by_status(isolated_repo):
    await isolated_repo.create(_sample_scan(server_path="/srv/a", status="completed"))
    await isolated_repo.create(_sample_scan(server_path="/srv/b", status="failed"))
    failed = await isolated_repo.query_by_status("failed")
    assert len(failed) == 1
    assert failed[0]["server_path"] == "/srv/b"


async def test_load_all_does_not_raise(isolated_repo):
    await isolated_repo.create(_sample_scan())
    await isolated_repo.load_all()
