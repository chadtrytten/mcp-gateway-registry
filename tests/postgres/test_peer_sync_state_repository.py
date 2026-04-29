"""Integration tests for PostgresPeerSyncStateStore.

Skips in CI when no DB is available. Set ``POSTGRES_TEST_DSN`` to enable.

Note: the production schema (``postgres-B-tables-008``) has an FK on
``mcp_peer_sync_state.id`` referencing ``mcp_peers.id``. The fixture below
omits the FK so this test exercises the store in isolation; the
peer_federation tests cover the FK CASCADE behavior.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.peer_sync_state_repository import (
    TABLE_DDL,
    TRIGGER_DDL,
    PostgresPeerSyncStateStore,
)
from registry.schemas.peer_federation_schema import PeerSyncStatus

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason="Requires Postgres running — set POSTGRES_TEST_DSN to enable.",
    ),
]


_MCP_SET_UPDATED_AT_FN = """
CREATE OR REPLACE FUNCTION mcp_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


@pytest.fixture
async def isolated_store(monkeypatch):
    assert POSTGRES_TEST_DSN

    schema = f"test_pgsync_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_peer_sync_state_default'
    async with pool.acquire() as conn:
        await conn.execute(TABLE_DDL.format(table=table))
        await conn.execute(TRIGGER_DDL.format(table=table))

    from registry.repositories.postgres import peer_sync_state_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    store = PostgresPeerSyncStateStore()
    try:
        yield store
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def test_get_returns_none_when_missing(isolated_store):
    assert await isolated_store.get("nope") is None


async def test_upsert_then_get_round_trips(isolated_store):
    status = PeerSyncStatus(
        peer_id="central-registry",
        is_healthy=True,
        current_generation=42,
        consecutive_failures=0,
    )
    saved = await isolated_store.upsert("central-registry", status)
    assert saved is status

    fetched = await isolated_store.get("central-registry")
    assert fetched is not None
    assert fetched.peer_id == "central-registry"
    assert fetched.is_healthy is True
    assert fetched.current_generation == 42


async def test_upsert_replaces_existing(isolated_store):
    """Second upsert overwrites — exposes the watermark, not history."""
    await isolated_store.upsert(
        "p1", PeerSyncStatus(peer_id="p1", current_generation=1)
    )
    await isolated_store.upsert(
        "p1", PeerSyncStatus(peer_id="p1", current_generation=99, is_healthy=True)
    )
    fetched = await isolated_store.get("p1")
    assert fetched is not None
    assert fetched.current_generation == 99
    assert fetched.is_healthy is True


async def test_list_all_returns_every_row(isolated_store):
    await isolated_store.upsert("a", PeerSyncStatus(peer_id="a"))
    await isolated_store.upsert("b", PeerSyncStatus(peer_id="b"))
    await isolated_store.upsert("c", PeerSyncStatus(peer_id="c"))
    rows = await isolated_store.list_all()
    assert {r.peer_id for r in rows} == {"a", "b", "c"}


async def test_count_matches_list_all(isolated_store):
    assert await isolated_store.count() == 0
    await isolated_store.upsert("a", PeerSyncStatus(peer_id="a"))
    await isolated_store.upsert("b", PeerSyncStatus(peer_id="b"))
    assert await isolated_store.count() == 2


async def test_delete_removes_row(isolated_store):
    await isolated_store.upsert("p", PeerSyncStatus(peer_id="p"))
    assert await isolated_store.delete("p") is True
    assert await isolated_store.get("p") is None
    # Second delete is a no-op (returns False).
    assert await isolated_store.delete("p") is False
