"""Integration tests for PostgresPeerFederationRepository.

Skips in CI when no DB is available. Set ``POSTGRES_TEST_DSN`` to enable.

The fixture creates BOTH ``mcp_peers_default`` and
``mcp_peer_sync_state_default`` (with the FK CASCADE) so we can verify the
peer-delete → sync-state-delete cascade behavior that distinguishes the
Postgres impl from the documentdb impl.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.peer_federation_repository import (
    TABLE_DDL as PEERS_DDL,
    TRIGGER_DDL as PEERS_TRIGGER_DDL,
    PostgresPeerFederationRepository,
)
from registry.repositories.postgres.peer_sync_state_repository import (
    TABLE_DDL as SYNC_DDL,
    TRIGGER_DDL as SYNC_TRIGGER_DDL,
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


# Sync-state table with the production FK so we can exercise CASCADE.
_SYNC_DDL_WITH_FK = """
CREATE TABLE IF NOT EXISTS {sync_table} (
    id          TEXT PRIMARY KEY
                    REFERENCES {peers_table}(id) ON DELETE CASCADE,
    data        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN

    schema = f"test_pgpeerfed_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    peers_table = f'"{schema}".mcp_peers_default'
    sync_table = f'"{schema}".mcp_peer_sync_state_default'

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(PEERS_DDL.format(table=peers_table))
        await conn.execute(PEERS_TRIGGER_DDL.format(table=peers_table))
        await conn.execute(
            _SYNC_DDL_WITH_FK.format(sync_table=sync_table, peers_table=peers_table)
        )
        await conn.execute(SYNC_TRIGGER_DDL.format(table=sync_table))

    from registry.repositories.postgres import peer_federation_repository as mod
    from registry.repositories.postgres import peer_sync_state_repository as sync_mod

    async def _patched_pool():
        return pool

    def _patched_table(base):
        return f'"{schema}".{base}_default'

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", _patched_table)
    monkeypatch.setattr(sync_mod, "get_pool", _patched_pool)
    monkeypatch.setattr(sync_mod, "table_name", _patched_table)

    repo = PostgresPeerFederationRepository()
    try:
        yield repo, pool, schema
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _peer_kwargs(peer_id: str = "central-registry", **overrides):
    base = dict(
        peer_id=peer_id,
        name="Central Registry",
        endpoint="https://central.registry.example.test",
        enabled=True,
        sync_mode="all",
        sync_interval_minutes=30,
    )
    base.update(overrides)
    return base


async def test_get_peer_returns_none_when_missing(isolated_repo):
    repo, _pool, _schema = isolated_repo
    assert await repo.get_peer("nope") is None


async def test_create_peer_then_get_round_trips(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    config = PeerRegistryConfig(**_peer_kwargs())
    saved = await repo.create_peer(config)
    assert saved.peer_id == "central-registry"

    fetched = await repo.get_peer("central-registry")
    assert fetched is not None
    assert fetched.peer_id == "central-registry"
    assert fetched.name == "Central Registry"
    assert fetched.enabled is True
    assert fetched.endpoint.rstrip("/") == "https://central.registry.example.test"


async def test_create_peer_seeds_initial_sync_status(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p1")))
    status = await repo.get_sync_status("p1")
    assert status is not None
    assert status.peer_id == "p1"
    assert status.consecutive_failures == 0


async def test_create_peer_rejects_duplicate(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("dup")))
    with pytest.raises(ValueError, match="already exists"):
        await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("dup")))


async def test_list_peers_filters_by_enabled(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("on1", enabled=True)))
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("on2", enabled=True)))
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("off1", enabled=False)))

    all_peers = await repo.list_peers()
    enabled_peers = await repo.list_peers(enabled=True)
    disabled_peers = await repo.list_peers(enabled=False)

    assert {p.peer_id for p in all_peers} == {"on1", "on2", "off1"}
    assert {p.peer_id for p in enabled_peers} == {"on1", "on2"}
    assert {p.peer_id for p in disabled_peers} == {"off1"}


async def test_update_peer_merges_fields_and_persists(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p1")))
    updated = await repo.update_peer(
        "p1", {"name": "Renamed", "sync_interval_minutes": 60}
    )
    assert updated.name == "Renamed"
    assert updated.sync_interval_minutes == 60

    refetched = await repo.get_peer("p1")
    assert refetched is not None
    assert refetched.name == "Renamed"
    assert refetched.sync_interval_minutes == 60


async def test_update_missing_peer_raises(isolated_repo):
    repo, _pool, _schema = isolated_repo
    with pytest.raises(ValueError, match="not found"):
        await repo.update_peer("ghost", {"name": "Nope"})


async def test_delete_peer_cascades_to_sync_state(isolated_repo):
    """FK ON DELETE CASCADE removes sync_state when peer is deleted."""
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, pool, schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p1")))
    assert await repo.get_sync_status("p1") is not None

    assert await repo.delete_peer("p1") is True
    assert await repo.get_peer("p1") is None
    assert await repo.get_sync_status("p1") is None

    # Direct DB confirmation: no orphaned sync_state row.
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT COUNT(*)::BIGINT FROM "{schema}".mcp_peer_sync_state_default '
            f"WHERE id = $1",
            "p1",
        )
    assert count == 0


async def test_delete_missing_peer_raises(isolated_repo):
    repo, _pool, _schema = isolated_repo
    with pytest.raises(ValueError, match="not found"):
        await repo.delete_peer("ghost")


async def test_update_sync_status_upserts(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p1")))

    new_status = PeerSyncStatus(
        peer_id="p1",
        is_healthy=True,
        current_generation=99,
    )
    await repo.update_sync_status("p1", new_status)

    fetched = await repo.get_sync_status("p1")
    assert fetched is not None
    assert fetched.current_generation == 99
    assert fetched.is_healthy is True


async def test_list_sync_statuses_returns_every_seeded_peer(isolated_repo):
    from registry.schemas.peer_federation_schema import PeerRegistryConfig

    repo, _pool, _schema = isolated_repo
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p1")))
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p2")))
    await repo.create_peer(PeerRegistryConfig(**_peer_kwargs("p3")))

    statuses = await repo.list_sync_statuses()
    assert {s.peer_id for s in statuses} == {"p1", "p2", "p3"}


async def test_load_all_does_not_raise(isolated_repo):
    repo, _pool, _schema = isolated_repo
    await repo.load_all()
