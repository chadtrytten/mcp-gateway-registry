"""Integration tests for PostgresFederationConfigRepository.

Pattern matches ``tests/postgres/test_registry_card_repository.py``: skips in
CI when no Postgres is available; per-test isolated schema.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.federation_config_repository import (
    PostgresFederationConfigRepository,
)
from registry.schemas.federation_schema import FederationConfig

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
    """Schema-qualified DDL mirroring postgres-B-tables-006-federation_config.sql."""
    return f"""
CREATE TABLE IF NOT EXISTS {qualified} (
    id          TEXT PRIMARY KEY,
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

    schema = f"test_pgfc_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    qualified = f'"{schema}".mcp_federation_config_default'
    async with pool.acquire() as conn:
        await conn.execute(_table_ddl(qualified))
        await conn.execute(_trigger_ddl(
            qualified, f'"{schema}"."mcp_federation_config_default_updated_at"'
        ))

    from registry.repositories.postgres import federation_config_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda base: f'"{schema}".{base}_default')

    repo = PostgresFederationConfigRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_config(
    *,
    anthropic_enabled: bool = True,
    asor_enabled: bool = False,
    aws_enabled: bool = False,
) -> FederationConfig:
    cfg = FederationConfig()
    cfg.anthropic.enabled = anthropic_enabled
    cfg.asor.enabled = asor_enabled
    cfg.aws_registry.enabled = aws_enabled
    return cfg


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


async def test_get_config_returns_none_when_absent(isolated_repo):
    assert await isolated_repo.get_config() is None
    assert await isolated_repo.get_config("custom") is None


async def test_save_then_get_round_trips(isolated_repo):
    cfg = _sample_config(anthropic_enabled=True, asor_enabled=True)
    saved = await isolated_repo.save_config(cfg)
    assert saved is cfg

    fetched = await isolated_repo.get_config()
    assert fetched is not None
    assert fetched.anthropic.enabled is True
    assert fetched.asor.enabled is True
    assert fetched.aws_registry.enabled is False


async def test_save_config_supports_custom_id(isolated_repo):
    cfg = _sample_config()
    await isolated_repo.save_config(cfg, config_id="alt")
    assert await isolated_repo.get_config("default") is None
    assert await isolated_repo.get_config("alt") is not None


async def test_save_config_upsert_replaces_existing(isolated_repo):
    cfg1 = _sample_config(anthropic_enabled=True)
    await isolated_repo.save_config(cfg1)

    cfg2 = _sample_config(anthropic_enabled=False, aws_enabled=True)
    await isolated_repo.save_config(cfg2)

    fetched = await isolated_repo.get_config()
    assert fetched is not None
    assert fetched.anthropic.enabled is False
    assert fetched.aws_registry.enabled is True


async def test_delete_config_returns_true_then_false(isolated_repo):
    await isolated_repo.save_config(_sample_config())
    assert await isolated_repo.delete_config() is True
    assert await isolated_repo.delete_config() is False


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


async def test_list_configs_returns_summaries(isolated_repo):
    await isolated_repo.save_config(_sample_config(), config_id="default")
    await isolated_repo.save_config(_sample_config(), config_id="alt")

    listed = await isolated_repo.list_configs()
    ids = {c["id"] for c in listed}
    assert ids == {"default", "alt"}
    for entry in listed:
        assert entry["created_at"] is not None
        assert entry["updated_at"] is not None
        # ISO-8601 UTC string with trailing Z.
        assert entry["created_at"].endswith("Z")


async def test_list_configs_empty(isolated_repo):
    assert await isolated_repo.list_configs() == []


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


async def test_get_with_dead_pool_returns_none(monkeypatch):
    from registry.repositories.postgres import federation_config_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresFederationConfigRepository()
    assert await repo.get_config() is None
    assert await repo.list_configs() == []
    assert await repo.delete_config() is False


async def test_save_with_dead_pool_raises(monkeypatch):
    """Connection errors on save() bubble up to the caller."""
    from registry.repositories.postgres import federation_config_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresFederationConfigRepository()
    with pytest.raises(asyncpg.PostgresConnectionError):
        await repo.save_config(_sample_config())
