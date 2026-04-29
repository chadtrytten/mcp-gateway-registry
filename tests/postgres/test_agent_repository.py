"""Integration tests for PostgresAgentRepository.

Skipped in CI when no DB is available; set ``POSTGRES_TEST_DSN`` to enable.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.agent_repository import (
    PostgresAgentRepository,
)
from registry.schemas.agent_models import AgentCard

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
    name        TEXT GENERATED ALWAYS AS (data->>'name')        STORED,
    visibility  TEXT GENERATED ALWAYS AS (data->>'visibility')  STORED,
    is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {idx}_data_gin ON {table} USING GIN (data jsonb_path_ops);
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
    schema = f"test_pgagent_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_agents_default'
    idx = f'"{schema}".mcp_agents_default'.replace('"', "").replace(".", "_")
    async with pool.acquire() as conn:
        await conn.execute(_TABLE_DDL.format(table=table, idx=idx))
        await conn.execute(_TRIGGER_DDL.format(table=table, idx=idx))

    from registry.repositories.postgres import agent_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    repo = PostgresAgentRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_card(path: str = "/agents/test", name: str = "Test Agent") -> AgentCard:
    return AgentCard(
        name=name,
        description="A test agent",
        url="https://agents.example.test/test",
        version="1.0.0",
        path=path,
        visibility="public",
    )


async def test_get_returns_none_when_missing(isolated_repo):
    assert await isolated_repo.get("/agents/nope") is None


async def test_create_then_get_round_trips(isolated_repo):
    saved = await isolated_repo.create(_sample_card())
    assert saved.path == "/agents/test"
    fetched = await isolated_repo.get("/agents/test")
    assert fetched is not None
    assert fetched.name == "Test Agent"
    assert fetched.path == "/agents/test"


async def test_create_duplicate_raises_value_error(isolated_repo):
    await isolated_repo.create(_sample_card())
    with pytest.raises(ValueError):
        await isolated_repo.create(_sample_card())


async def test_list_all_returns_cards(isolated_repo):
    await isolated_repo.create(_sample_card("/agents/a", "A"))
    await isolated_repo.create(_sample_card("/agents/b", "B"))
    cards = await isolated_repo.list_all()
    assert {c.path for c in cards} == {"/agents/a", "/agents/b"}


async def test_list_paginated(isolated_repo):
    for i in range(5):
        await isolated_repo.create(_sample_card(f"/agents/a-{i}", f"A{i}"))
    page = await isolated_repo.list_paginated(skip=2, limit=2)
    assert len(page) == 2


async def test_update_patches_card(isolated_repo):
    await isolated_repo.create(_sample_card())
    updated = await isolated_repo.update(
        "/agents/test", {"description": "renamed"}
    )
    assert updated.description == "renamed"
    fetched = await isolated_repo.get("/agents/test")
    assert fetched.description == "renamed"


async def test_update_missing_raises(isolated_repo):
    with pytest.raises(ValueError):
        await isolated_repo.update("/agents/nope", {"description": "x"})


async def test_delete_returns_true_then_false(isolated_repo):
    await isolated_repo.create(_sample_card())
    assert await isolated_repo.delete("/agents/test") is True
    assert await isolated_repo.delete("/agents/test") is False


async def test_get_state_per_path_and_global(isolated_repo):
    await isolated_repo.create(_sample_card("/agents/a"))
    await isolated_repo.create(_sample_card("/agents/b"))
    assert await isolated_repo.get_state("/agents/a") is False
    assert await isolated_repo.set_state("/agents/a", True) is True
    state = await isolated_repo.get_state()
    assert isinstance(state, dict)
    assert "/agents/a" in state["enabled"]
    assert "/agents/b" in state["disabled"]


async def test_set_state_returns_false_when_missing(isolated_repo):
    assert await isolated_repo.set_state("/agents/nope", True) is False


async def test_save_state_no_op(isolated_repo):
    await isolated_repo.save_state({"enabled": ["/x"], "disabled": []})


async def test_count(isolated_repo):
    assert await isolated_repo.count() == 0
    await isolated_repo.create(_sample_card("/agents/a"))
    await isolated_repo.create(_sample_card("/agents/b"))
    assert await isolated_repo.count() == 2


async def test_update_field_set_and_unset(isolated_repo):
    await isolated_repo.create(_sample_card())
    assert await isolated_repo.update_field("/agents/test", "license", "MIT") is True
    fetched = await isolated_repo.get("/agents/test")
    assert fetched.license == "MIT"
    assert await isolated_repo.update_field("/agents/test", "license", None) is True
    fetched = await isolated_repo.get("/agents/test")
    # AgentCard schema gives `license` a default of "N/A" when absent.
    assert fetched.license == "N/A"


async def test_load_all_does_not_raise(isolated_repo):
    await isolated_repo.create(_sample_card())
    await isolated_repo.load_all()


async def test_find_with_filter_equality(isolated_repo):
    await isolated_repo.create(_sample_card("/agents/a", "A"))
    await isolated_repo.create(_sample_card("/agents/b", "B"))
    matches = await isolated_repo.find_with_filter({"name": "A"})
    assert set(matches.keys()) == {"/agents/a"}
