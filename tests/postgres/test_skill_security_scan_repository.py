"""Integration tests for PostgresSkillSecurityScanRepository.

Skips in CI when no DB is available — set ``POSTGRES_TEST_DSN`` to enable,
matching the pattern in ``tests/postgres/test_registry_card_repository.py``.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.skill_security_scan_repository import (
    TABLE_DDL,
    PostgresSkillSecurityScanRepository,
)

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason=(
            "Requires Postgres running — set POSTGRES_TEST_DSN to enable. "
            "Pattern matches tests/postgres/test_registry_card_repository.py."
        ),
    ),
]


@pytest.fixture
async def isolated_repo(monkeypatch):
    """Repo wired to a per-test schema. CASCADE drop on teardown."""
    assert POSTGRES_TEST_DSN

    schema = f"test_pgskillscan_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_skill_security_scans_default'
    async with pool.acquire() as conn:
        await conn.execute(TABLE_DDL.format(table=table))

    from registry.repositories.postgres import skill_security_scan_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    repo = PostgresSkillSecurityScanRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _scan(skill_path: str = "/skills/pdf-processing", **overrides):
    return {
        "skill_path": skill_path,
        "scan_status": "completed",
        "scan_timestamp": "2026-04-29T10:00:00Z",
        "total_vulnerabilities": 3,
        "critical_count": 1,
        "high_count": 0,
        "medium_count": 1,
        "low_count": 1,
        "scanner_version": "trivy@0.50",
        **overrides,
    }


async def test_get_returns_none_when_table_empty(isolated_repo):
    assert await isolated_repo.get("/skills/none") is None


async def test_create_then_get_latest_round_trips_payload(isolated_repo):
    assert await isolated_repo.create(_scan()) is True
    fetched = await isolated_repo.get_latest("/skills/pdf-processing")
    assert fetched is not None
    assert fetched["skill_path"] == "/skills/pdf-processing"
    assert fetched["scan_status"] == "completed"
    assert fetched["total_vulnerabilities"] == 3
    assert fetched["scanner_version"] == "trivy@0.50"


async def test_create_rejects_missing_skill_path(isolated_repo):
    bad = {"scan_status": "completed"}
    assert await isolated_repo.create(bad) is False


async def test_get_latest_returns_newest_of_many(isolated_repo):
    """A skill with multiple scans returns the newest by scan_timestamp."""
    await isolated_repo.create(
        _scan(scan_timestamp="2026-04-27T10:00:00Z", scanner_version="v1")
    )
    await isolated_repo.create(
        _scan(scan_timestamp="2026-04-29T12:00:00Z", scanner_version="v3")
    )
    await isolated_repo.create(
        _scan(scan_timestamp="2026-04-28T10:00:00Z", scanner_version="v2")
    )
    latest = await isolated_repo.get_latest("/skills/pdf-processing")
    assert latest is not None
    assert latest["scanner_version"] == "v3"


async def test_list_all_orders_newest_first(isolated_repo):
    await isolated_repo.create(_scan("/skills/a", scan_timestamp="2026-04-27T10:00:00Z"))
    await isolated_repo.create(_scan("/skills/b", scan_timestamp="2026-04-29T10:00:00Z"))
    await isolated_repo.create(_scan("/skills/c", scan_timestamp="2026-04-28T10:00:00Z"))
    rows = await isolated_repo.list_all()
    assert [r["skill_path"] for r in rows] == ["/skills/b", "/skills/c", "/skills/a"]


async def test_query_by_status_filters(isolated_repo):
    await isolated_repo.create(_scan("/skills/a", scan_status="completed"))
    await isolated_repo.create(_scan("/skills/b", scan_status="failed"))
    await isolated_repo.create(_scan("/skills/c", scan_status="completed"))
    completed = await isolated_repo.query_by_status("completed")
    failed = await isolated_repo.query_by_status("failed")
    assert {r["skill_path"] for r in completed} == {"/skills/a", "/skills/c"}
    assert {r["skill_path"] for r in failed} == {"/skills/b"}


async def test_get_aliases_get_latest(isolated_repo):
    await isolated_repo.create(_scan(scanner_version="v9"))
    via_get = await isolated_repo.get("/skills/pdf-processing")
    via_latest = await isolated_repo.get_latest("/skills/pdf-processing")
    assert via_get == via_latest


async def test_load_all_does_not_raise_on_empty_table(isolated_repo):
    await isolated_repo.load_all()  # smoke — no return
