"""Integration tests for PostgresServerRepository.

Skipped in CI when no DB is available; set ``POSTGRES_TEST_DSN`` to enable
(matches the F/RegistryCard fixture pattern at
``tests/postgres/test_registry_card_repository.py``).
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.server_repository import (
    PostgresServerRepository,
)

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


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id          TEXT PRIMARY KEY,
    server_name TEXT GENERATED ALWAYS AS (data->>'server_name')             STORED,
    source      TEXT GENERATED ALWAYS AS (data->>'source')                  STORED,
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    status      TEXT GENERATED ALWAYS AS (COALESCE(data->>'status','active')) STORED,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {idx}_data_gin ON {table} USING GIN (data jsonb_path_ops);
CREATE INDEX IF NOT EXISTS {idx}_source_btree ON {table} (source) WHERE source IS NOT NULL;
"""

_TRIGGER_DDL = """
DROP TRIGGER IF EXISTS {idx}_updated_at ON {table};
CREATE TRIGGER {idx}_updated_at
    BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN
    schema = f"test_pgsrv_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_servers_default'
    idx = f'"{schema}".mcp_servers_default'.replace('"', "").replace(".", "_")
    async with pool.acquire() as conn:
        await conn.execute(_TABLE_DDL.format(table=table, idx=idx))
        await conn.execute(_TRIGGER_DDL.format(table=table, idx=idx))

    from registry.repositories.postgres import server_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    repo = PostgresServerRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_server(path: str = "/example", name: str = "Example") -> dict:
    return {
        "path": path,
        "server_name": name,
        "description": "test server",
        "source": "anthropic",
        "status": "active",
        "is_enabled": False,
    }


async def test_get_returns_none_when_missing(isolated_repo):
    assert await isolated_repo.get("/nope") is None


async def test_create_then_get_round_trips(isolated_repo):
    assert await isolated_repo.create(_sample_server()) is True
    fetched = await isolated_repo.get("/example")
    assert fetched is not None
    assert fetched["path"] == "/example"
    assert fetched["server_name"] == "Example"


async def test_create_duplicate_returns_false(isolated_repo):
    assert await isolated_repo.create(_sample_server()) is True
    assert await isolated_repo.create(_sample_server()) is False


async def test_list_all_returns_path_keyed_dict(isolated_repo):
    await isolated_repo.create(_sample_server("/a", "A"))
    await isolated_repo.create(_sample_server("/b", "B"))
    everything = await isolated_repo.list_all()
    assert set(everything.keys()) == {"/a", "/b"}
    assert everything["/a"]["server_name"] == "A"


async def test_list_paginated_offset_limit(isolated_repo):
    for i in range(5):
        await isolated_repo.create(_sample_server(f"/srv-{i}", f"S{i}"))
    page = await isolated_repo.list_paginated(skip=2, limit=2)
    assert len(page) == 2


async def test_list_by_source_filters_by_generated_column(isolated_repo):
    await isolated_repo.create({**_sample_server("/a"), "source": "anthropic"})
    await isolated_repo.create({**_sample_server("/b"), "source": "openai"})
    only = await isolated_repo.list_by_source("anthropic")
    assert set(only.keys()) == {"/a"}


async def test_update_replaces_body(isolated_repo):
    await isolated_repo.create(_sample_server())
    upd = _sample_server()
    upd["server_name"] = "Renamed"
    assert await isolated_repo.update("/example", upd) is True
    fetched = await isolated_repo.get("/example")
    assert fetched["server_name"] == "Renamed"


async def test_update_returns_false_when_missing(isolated_repo):
    assert await isolated_repo.update("/nope", _sample_server("/nope")) is False


async def test_delete_returns_true_on_hit_false_on_miss(isolated_repo):
    await isolated_repo.create(_sample_server())
    assert await isolated_repo.delete("/example") is True
    assert await isolated_repo.delete("/example") is False


async def test_delete_with_versions_clears_version_rows(isolated_repo):
    await isolated_repo.create(_sample_server("/foo"))
    await isolated_repo.create(_sample_server("/foo:v2"))
    await isolated_repo.create(_sample_server("/foo:v3"))
    deleted = await isolated_repo.delete_with_versions("/foo")
    assert deleted == 3


async def test_get_state_and_set_state_round_trip(isolated_repo):
    await isolated_repo.create(_sample_server())
    assert await isolated_repo.get_state("/example") is False
    assert await isolated_repo.set_state("/example", True) is True
    assert await isolated_repo.get_state("/example") is True
    fetched = await isolated_repo.get("/example")
    assert fetched["is_enabled"] is True


async def test_count_reflects_inserts(isolated_repo):
    assert await isolated_repo.count() == 0
    await isolated_repo.create(_sample_server("/a"))
    await isolated_repo.create(_sample_server("/b"))
    assert await isolated_repo.count() == 2


async def test_update_field_sets_top_level(isolated_repo):
    await isolated_repo.create(_sample_server())
    assert await isolated_repo.update_field("/example", "description", "newdesc") is True
    fetched = await isolated_repo.get("/example")
    assert fetched["description"] == "newdesc"


async def test_update_field_unset_with_none(isolated_repo):
    await isolated_repo.create(_sample_server())
    assert await isolated_repo.update_field("/example", "description", None) is True
    fetched = await isolated_repo.get("/example")
    assert "description" not in fetched


async def test_load_all_does_not_raise(isolated_repo):
    await isolated_repo.create(_sample_server())
    await isolated_repo.load_all()


async def test_find_with_filter_equality(isolated_repo):
    await isolated_repo.create({**_sample_server("/a"), "source": "anthropic"})
    await isolated_repo.create({**_sample_server("/b"), "source": "openai"})
    matches = await isolated_repo.find_with_filter({"source": "openai"})
    assert set(matches.keys()) == {"/b"}
