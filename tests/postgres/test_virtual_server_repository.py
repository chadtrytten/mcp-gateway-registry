"""Integration tests for PostgresVirtualServerRepository.

Pattern matches ``tests/postgres/test_registry_card_repository.py``: skips in
CI when no Postgres is available; per-test isolated schema.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.exceptions import VirtualServerAlreadyExistsError
from registry.repositories.postgres.virtual_server_repository import (
    PostgresVirtualServerRepository,
)
from registry.schemas.virtual_server_models import VirtualServerConfig

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


def _table_ddl(qualified: str) -> str:
    """Schema-qualified DDL mirroring postgres-B-tables-011-virtual_servers.sql."""
    return f"""
CREATE TABLE IF NOT EXISTS {qualified} (
    id          TEXT PRIMARY KEY,
    server_name TEXT GENERATED ALWAYS AS (data->>'server_name') STORED,
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    tags        TEXT[] NOT NULL DEFAULT '{{}}'::text[],
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _trigger_ddl(qualified: str, trigger_name: str) -> str:
    return f"""
DROP TRIGGER IF EXISTS {trigger_name} ON {qualified};
CREATE TRIGGER {trigger_name}
    BEFORE UPDATE ON {qualified}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN

    schema = f"test_pgvs_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    qualified = f'"{schema}".virtual_servers_default'
    async with pool.acquire() as conn:
        await conn.execute(_table_ddl(qualified))
        await conn.execute(_trigger_ddl(qualified, f'"{schema}"."virtual_servers_default_updated_at"'))

    from registry.repositories.postgres import virtual_server_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda base: f'"{schema}".{base}_default')

    repo = PostgresVirtualServerRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_config(
    path: str = "/virtual/dev-essentials",
    server_name: str = "dev-essentials",
    *,
    is_enabled: bool = True,
    tags: list[str] | None = None,
) -> VirtualServerConfig:
    return VirtualServerConfig(
        path=path,
        server_name=server_name,
        description="A test virtual server",
        is_enabled=is_enabled,
        tags=tags or ["dev"],
    )


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


async def test_get_returns_none_when_absent(isolated_repo):
    assert await isolated_repo.get("/virtual/nope") is None


async def test_create_then_get_round_trips(isolated_repo):
    cfg = _sample_config()
    saved = await isolated_repo.create(cfg)
    assert saved is cfg

    fetched = await isolated_repo.get(cfg.path)
    assert fetched is not None
    assert fetched.path == cfg.path
    assert fetched.server_name == cfg.server_name
    assert fetched.is_enabled is True
    assert sorted(fetched.tags) == sorted(cfg.tags)


async def test_create_raises_already_exists(isolated_repo):
    cfg = _sample_config()
    await isolated_repo.create(cfg)
    with pytest.raises(VirtualServerAlreadyExistsError):
        await isolated_repo.create(cfg)


async def test_update_partial_merges_into_data_and_hot_columns(isolated_repo):
    cfg = _sample_config(tags=["dev"])
    await isolated_repo.create(cfg)

    updated = await isolated_repo.update(
        cfg.path,
        {"description": "Updated", "tags": ["prod"], "is_enabled": False},
    )
    assert updated is not None
    assert updated.description == "Updated"
    assert sorted(updated.tags) == ["prod"]
    assert updated.is_enabled is False
    assert updated.server_name == cfg.server_name


async def test_update_returns_none_when_absent(isolated_repo):
    assert await isolated_repo.update("/virtual/missing", {"server_name": "x"}) is None


async def test_delete_returns_true_then_false(isolated_repo):
    cfg = _sample_config()
    await isolated_repo.create(cfg)
    assert await isolated_repo.delete(cfg.path) is True
    assert await isolated_repo.delete(cfg.path) is False


async def test_get_state_and_set_state(isolated_repo):
    cfg = _sample_config(is_enabled=False)
    await isolated_repo.create(cfg)
    assert await isolated_repo.get_state(cfg.path) is False
    assert await isolated_repo.set_state(cfg.path, True) is True
    assert await isolated_repo.get_state(cfg.path) is True
    # No-op when already in target state.
    assert await isolated_repo.set_state(cfg.path, True) is False


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


async def test_list_all_returns_everything(isolated_repo):
    enabled = _sample_config(path="/virtual/a", server_name="a", is_enabled=True)
    disabled = _sample_config(path="/virtual/b", server_name="b", is_enabled=False)
    await isolated_repo.create(enabled)
    await isolated_repo.create(disabled)

    listed = await isolated_repo.list_all()
    assert {c.path for c in listed} == {"/virtual/a", "/virtual/b"}


async def test_list_enabled_filters_disabled(isolated_repo):
    enabled = _sample_config(path="/virtual/a", server_name="a", is_enabled=True)
    disabled = _sample_config(path="/virtual/b", server_name="b", is_enabled=False)
    await isolated_repo.create(enabled)
    await isolated_repo.create(disabled)

    listed = await isolated_repo.list_enabled()
    assert {c.path for c in listed} == {"/virtual/a"}


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


async def test_get_with_dead_pool_returns_none(monkeypatch):
    from registry.repositories.postgres import virtual_server_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresVirtualServerRepository()
    assert await repo.get("/virtual/x") is None
    assert await repo.list_all() == []
