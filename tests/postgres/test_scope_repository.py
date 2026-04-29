"""Integration tests for PostgresScopeRepository.

Mirrors ``tests/postgres/test_registry_card_repository.py`` (POSTGRES-F).
One test per public ABC method (16 abstract + ``list_groups``) plus a
focused atomic-add / atomic-remove / idempotency test for each JSONB array
operation, per BX-4 instructions.
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.scope_repository import (
    PostgresScopeRepository,
)

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason="Requires Postgres — set POSTGRES_TEST_DSN to enable.",
    ),
]


_PRELUDE_FNS = """
CREATE OR REPLACE FUNCTION mcp_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mcp_jsonb_array_remove_value(arr JSONB, val JSONB)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT COALESCE(
        (SELECT jsonb_agg(elem)
         FROM jsonb_array_elements(COALESCE(arr, '[]'::jsonb)) elem
         WHERE elem <> val),
        '[]'::jsonb
    );
$$;

CREATE OR REPLACE FUNCTION mcp_jsonb_array_add_unique(arr JSONB, val JSONB)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN COALESCE(arr, '[]'::jsonb) @> jsonb_build_array(val)
            THEN COALESCE(arr, '[]'::jsonb)
        ELSE COALESCE(arr, '[]'::jsonb) || jsonb_build_array(val)
    END;
$$;
"""


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id              TEXT PRIMARY KEY,
    ui_permissions  JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    server_access   JSONB NOT NULL DEFAULT '[]'::jsonb,
    group_mappings  JSONB NOT NULL DEFAULT '[]'::jsonb,
    description     TEXT,
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TRIGGER {trigger} BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN
    schema = f"test_pgscr_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_PRELUDE_FNS)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".mcp_scopes_default'
    trigger = f"trg_mcp_scopes_{schema[:24]}"
    async with pool.acquire() as conn:
        await conn.execute(_TABLE_DDL.format(table=table, trigger=trigger))

    from registry.repositories.postgres import scope_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda base: f'"{schema}".{base}_default')

    repo = PostgresScopeRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


# ------------------------------------------------------------------- ABC #1-#4

async def test_load_all_handles_empty_table(isolated_repo):
    await isolated_repo.load_all()
    assert isolated_repo._scopes_cache == {"UI-Scopes": {}, "group_mappings": {}}


async def test_create_then_load_all_populates_cache(isolated_repo):
    await isolated_repo.create_group("g1", description="grp 1")
    await isolated_repo.add_group_mapping("g1", "/keycloak/admins")
    await isolated_repo.add_server_to_ui_scopes("g1", "weather")
    await isolated_repo.load_all()
    cache = isolated_repo._scopes_cache
    # group_mappings is a reverse index: keycloak_group → [scope_names]
    assert cache["group_mappings"]["/keycloak/admins"] == ["g1"]
    assert "list_service" in cache["UI-Scopes"]["g1"]


async def test_get_ui_scopes_returns_dict(isolated_repo):
    await isolated_repo.create_group("g1")
    assert await isolated_repo.get_ui_scopes("g1") == {}
    await isolated_repo.add_server_to_ui_scopes("g1", "weather")
    scopes = await isolated_repo.get_ui_scopes("g1")
    assert scopes == {"list_service": ["weather"]}


async def test_get_ui_scopes_missing_group_returns_empty(isolated_repo):
    assert await isolated_repo.get_ui_scopes("missing") == {}


async def test_get_group_mappings_uses_containment(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.create_group("g2")
    await isolated_repo.add_group_mapping("g1", "/kc/admins")
    await isolated_repo.add_group_mapping("g2", "/kc/admins")
    await isolated_repo.add_group_mapping("g2", "/kc/users")

    admins = await isolated_repo.get_group_mappings("/kc/admins")
    users = await isolated_repo.get_group_mappings("/kc/users")
    nope = await isolated_repo.get_group_mappings("/kc/none")
    assert admins == ["g1", "g2"]
    assert users == ["g2"]
    assert nope == []


async def test_get_server_scopes_flattens_access_rules(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.add_server_scope(
        server_path="/weather", scope_name="g1", methods=["GET"], tools=["all"]
    )
    rules = await isolated_repo.get_server_scopes("g1")
    assert any(r["server"] == "weather" for r in rules)


# ------------------------------------------------------------------- ABC #5-#8

async def test_add_server_scope(isolated_repo):
    await isolated_repo.create_group("g1")
    ok = await isolated_repo.add_server_scope("/srv", "g1", ["GET"], ["all"])
    assert ok is True
    rules = await isolated_repo.get_server_scopes("g1")
    assert {"server": "srv", "methods": ["GET"], "tools": ["all"]} in rules


async def test_remove_server_scope(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.add_server_scope("/srv", "g1", ["GET"], ["all"])
    ok = await isolated_repo.remove_server_scope("/srv", "g1")
    assert ok is True
    assert await isolated_repo.get_server_scopes("g1") == []


async def test_create_group_and_idempotent_failure(isolated_repo):
    assert await isolated_repo.create_group("g1", description="d") is True
    # Duplicate create returns False (UniqueViolationError caught).
    assert await isolated_repo.create_group("g1") is False


async def test_delete_group(isolated_repo):
    await isolated_repo.create_group("g1")
    assert await isolated_repo.delete_group("g1") is True
    # Second delete: missing row → False.
    assert await isolated_repo.delete_group("g1") is False


# ------------------------------------------------------------------- ABC #9-#11

async def test_get_group_returns_full_doc(isolated_repo):
    await isolated_repo.create_group("g1", description="my group")
    doc = await isolated_repo.get_group("g1")
    assert doc is not None
    assert doc["scope_name"] == "g1"
    assert doc["description"] == "my group"
    assert doc["ui_permissions"] == {}
    assert doc["server_access"] == []
    assert doc["group_mappings"] == []
    assert doc["created_at"] is not None


async def test_get_group_missing_returns_none(isolated_repo):
    assert await isolated_repo.get_group("missing") is None


async def test_list_groups_includes_counts(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.add_server_scope("/a", "g1", ["GET"])
    await isolated_repo.add_server_scope("/b", "g1", ["POST"])
    listed = await isolated_repo.list_groups()
    assert "g1" in listed
    assert listed["g1"]["server_count"] == 2


async def test_group_exists_flips(isolated_repo):
    assert await isolated_repo.group_exists("g1") is False
    await isolated_repo.create_group("g1")
    assert await isolated_repo.group_exists("g1") is True


# --------------------------------------------------- ABC #12-#13: UI atomic ops

async def test_add_server_to_ui_scopes_atomic_and_idempotent(isolated_repo):
    await isolated_repo.create_group("g1")
    assert await isolated_repo.add_server_to_ui_scopes("g1", "weather") is True
    # Idempotent — second add must not duplicate.
    assert await isolated_repo.add_server_to_ui_scopes("g1", "weather") is True
    scopes = await isolated_repo.get_ui_scopes("g1")
    assert scopes["list_service"] == ["weather"]
    assert await isolated_repo.add_server_to_ui_scopes("g1", "currenttime") is True
    scopes = await isolated_repo.get_ui_scopes("g1")
    assert sorted(scopes["list_service"]) == ["currenttime", "weather"]


async def test_remove_server_from_ui_scopes_atomic_and_idempotent(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.add_server_to_ui_scopes("g1", "weather")
    await isolated_repo.add_server_to_ui_scopes("g1", "currenttime")
    assert await isolated_repo.remove_server_from_ui_scopes("g1", "weather") is True
    scopes = await isolated_repo.get_ui_scopes("g1")
    assert scopes["list_service"] == ["currenttime"]
    # Idempotent — removing an absent value returns True with no change.
    assert await isolated_repo.remove_server_from_ui_scopes("g1", "weather") is True
    scopes = await isolated_repo.get_ui_scopes("g1")
    assert scopes["list_service"] == ["currenttime"]


async def test_ui_scopes_ops_on_missing_group_return_false(isolated_repo):
    assert await isolated_repo.add_server_to_ui_scopes("missing", "x") is False
    assert await isolated_repo.remove_server_from_ui_scopes("missing", "x") is False


# --------------------------------------------------- ABC #14-#15: mapping ops

async def test_add_group_mapping_atomic_and_idempotent(isolated_repo):
    await isolated_repo.create_group("g1")
    assert await isolated_repo.add_group_mapping("g1", "/kc/admins") is True
    # Idempotent.
    assert await isolated_repo.add_group_mapping("g1", "/kc/admins") is True
    mappings = await isolated_repo.get_all_group_mappings()
    assert mappings["g1"] == ["/kc/admins"]


async def test_remove_group_mapping_atomic_and_idempotent(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.add_group_mapping("g1", "/kc/admins")
    await isolated_repo.add_group_mapping("g1", "/kc/users")
    assert await isolated_repo.remove_group_mapping("g1", "/kc/admins") is True
    mappings = await isolated_repo.get_all_group_mappings()
    assert mappings["g1"] == ["/kc/users"]
    # Idempotent.
    assert await isolated_repo.remove_group_mapping("g1", "/kc/admins") is True
    mappings = await isolated_repo.get_all_group_mappings()
    assert mappings["g1"] == ["/kc/users"]


async def test_group_mapping_ops_on_missing_group_return_false(isolated_repo):
    assert await isolated_repo.add_group_mapping("missing", "/kc/x") is False
    assert await isolated_repo.remove_group_mapping("missing", "/kc/x") is False


# ----------------------------------------------------------- ABC #16: bulk ops

async def test_get_all_group_mappings(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.create_group("g2")
    await isolated_repo.add_group_mapping("g1", "/kc/a")
    await isolated_repo.add_group_mapping("g2", "/kc/b")
    mappings = await isolated_repo.get_all_group_mappings()
    assert mappings == {"g1": ["/kc/a"], "g2": ["/kc/b"]}


async def test_add_server_to_multiple_scopes_transactional(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.create_group("g2")
    ok = await isolated_repo.add_server_to_multiple_scopes(
        "/srv", ["g1", "g2"], methods=["GET"], tools=["all"]
    )
    assert ok is True
    assert any(
        r["server"] == "srv" for r in await isolated_repo.get_server_scopes("g1")
    )
    assert any(
        r["server"] == "srv" for r in await isolated_repo.get_server_scopes("g2")
    )


async def test_remove_server_from_all_scopes(isolated_repo):
    await isolated_repo.create_group("g1")
    await isolated_repo.create_group("g2")
    await isolated_repo.add_server_scope("/srv", "g1", ["GET"])
    await isolated_repo.add_server_scope("/srv", "g2", ["POST"])
    ok = await isolated_repo.remove_server_from_all_scopes("/srv")
    assert ok is True
    assert await isolated_repo.get_server_scopes("g1") == []
    assert await isolated_repo.get_server_scopes("g2") == []


# ----------------------------------------------------- error-path: dead pool

async def test_get_with_dead_pool_returns_empty(monkeypatch):
    from registry.repositories.postgres import scope_repository as mod

    class _DeadPool:
        def acquire(self):
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresScopeRepository()
    assert await repo.get_ui_scopes("g") == {}
    assert await repo.get_group_mappings("/kc") == []
    assert await repo.get_server_scopes("g") == []
    assert await repo.list_groups() == {}
    assert await repo.group_exists("g") is False
