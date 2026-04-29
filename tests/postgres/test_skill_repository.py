"""Integration tests for PostgresSkillRepository.

Skips in CI when no Postgres is available. Two ways to run locally:

1. ``POSTGRES_TEST_DSN=postgresql:///mcp_test pytest tests/postgres -k skill``
2. testcontainers-postgres (sketch at the bottom of ``test_registry_card_repository.py``).

Each test runs in an isolated, randomly-named schema so parallel runs cannot
collide. The schema is dropped CASCADE on teardown.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import asyncpg
import pytest

from registry.exceptions import SkillAlreadyExistsError
from registry.repositories.postgres.skill_repository import PostgresSkillRepository
from registry.schemas.skill_models import SkillCard, VisibilityEnum

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
    """Schema-qualified DDL mirroring postgres-B-tables-009-agent_skills.sql."""
    return f"""
CREATE TABLE IF NOT EXISTS {qualified} (
    id              TEXT PRIMARY KEY,
    name            TEXT GENERATED ALWAYS AS (data->>'name')          STORED,
    visibility      TEXT GENERATED ALWAYS AS (data->>'visibility')    STORED,
    registry_name   TEXT GENERATED ALWAYS AS (data->>'registry_name') STORED,
    owner           TEXT GENERATED ALWAYS AS (data->>'owner')         STORED,
    is_enabled      BOOLEAN NOT NULL DEFAULT FALSE,
    tags            TEXT[] NOT NULL DEFAULT '{{}}'::text[],
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
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
    """A repo wired to a per-test schema in the test database."""
    assert POSTGRES_TEST_DSN

    schema = f"test_pgsk_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    qualified = f'"{schema}".agent_skills_default'
    async with pool.acquire() as conn:
        await conn.execute(_table_ddl(qualified))
        await conn.execute(_trigger_ddl(qualified, f'"{schema}"."agent_skills_default_updated_at"'))

    from registry.repositories.postgres import skill_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda base: f'"{schema}".{base}_default')

    repo = PostgresSkillRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_skill(
    path: str = "/skills/research-bot/summarize",
    name: str = "summarize",
    *,
    is_enabled: bool = True,
    tags: list[str] | None = None,
    visibility: VisibilityEnum = VisibilityEnum.PUBLIC,
    registry_name: str = "local",
    owner: str | None = None,
) -> SkillCard:
    return SkillCard(
        path=path,
        name=name,
        description="A test skill",
        skill_md_url="https://example.test/SKILL.md",
        is_enabled=is_enabled,
        tags=tags or ["llm", "rag"],
        visibility=visibility,
        registry_name=registry_name,
        owner=owner,
    )


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


async def test_get_returns_none_when_absent(isolated_repo):
    assert await isolated_repo.get("/skills/nope") is None


async def test_create_then_get_round_trips(isolated_repo):
    skill = _sample_skill()
    saved = await isolated_repo.create(skill)
    assert saved is skill

    fetched = await isolated_repo.get(skill.path)
    assert fetched is not None
    assert fetched.path == skill.path
    assert fetched.name == skill.name
    assert sorted(fetched.tags) == sorted(skill.tags)
    assert fetched.is_enabled is True


async def test_create_raises_skill_already_exists(isolated_repo):
    skill = _sample_skill()
    await isolated_repo.create(skill)
    with pytest.raises(SkillAlreadyExistsError):
        await isolated_repo.create(skill)


async def test_update_partial_merges_into_data_and_hot_columns(isolated_repo):
    skill = _sample_skill(tags=["a"])
    await isolated_repo.create(skill)

    updated = await isolated_repo.update(
        skill.path,
        {"description": "Updated", "tags": ["b", "c"], "is_enabled": False},
    )
    assert updated is not None
    assert updated.description == "Updated"
    assert sorted(updated.tags) == ["b", "c"]
    assert updated.is_enabled is False
    # Untouched fields preserved.
    assert updated.name == skill.name


async def test_update_returns_none_when_absent(isolated_repo):
    assert await isolated_repo.update("/skills/missing", {"name": "x"}) is None


async def test_delete_returns_true_then_false(isolated_repo):
    skill = _sample_skill()
    await isolated_repo.create(skill)
    assert await isolated_repo.delete(skill.path) is True
    assert await isolated_repo.delete(skill.path) is False


async def test_get_state_and_set_state(isolated_repo):
    skill = _sample_skill(is_enabled=False)
    await isolated_repo.create(skill)
    assert await isolated_repo.get_state(skill.path) is False
    assert await isolated_repo.set_state(skill.path, True) is True
    assert await isolated_repo.get_state(skill.path) is True
    # No-op when already in target state.
    assert await isolated_repo.set_state(skill.path, True) is False


# ---------------------------------------------------------------------------
# Listing & filtering
# ---------------------------------------------------------------------------


async def test_list_filtered_excludes_disabled_by_default(isolated_repo):
    enabled = _sample_skill(path="/skills/a", name="a", is_enabled=True)
    disabled = _sample_skill(path="/skills/b", name="b", is_enabled=False)
    await isolated_repo.create(enabled)
    await isolated_repo.create(disabled)

    listed = await isolated_repo.list_filtered()
    assert {s.path for s in listed} == {"/skills/a"}

    listed_all = await isolated_repo.list_filtered(include_disabled=True)
    assert {s.path for s in listed_all} == {"/skills/a", "/skills/b"}


async def test_list_filtered_by_tag(isolated_repo):
    a = _sample_skill(path="/skills/a", name="a", tags=["llm", "rag"])
    b = _sample_skill(path="/skills/b", name="b", tags=["search"])
    await isolated_repo.create(a)
    await isolated_repo.create(b)

    by_rag = await isolated_repo.list_filtered(tag="rag")
    assert {s.path for s in by_rag} == {"/skills/a"}


async def test_list_filtered_by_visibility_and_registry(isolated_repo):
    public = _sample_skill(path="/skills/a", name="a", visibility=VisibilityEnum.PUBLIC)
    private_remote = _sample_skill(
        path="/skills/b", name="b", visibility=VisibilityEnum.PRIVATE, registry_name="remote"
    )
    await isolated_repo.create(public)
    await isolated_repo.create(private_remote)

    public_only = await isolated_repo.list_filtered(visibility="public")
    assert {s.path for s in public_only} == {"/skills/a"}

    remote_only = await isolated_repo.list_filtered(registry_name="remote")
    assert {s.path for s in remote_only} == {"/skills/b"}


async def test_list_paginated_is_deterministic(isolated_repo):
    for i in range(5):
        await isolated_repo.create(_sample_skill(path=f"/skills/{i}", name=f"n{i}"))

    page1 = await isolated_repo.list_paginated(skip=0, limit=2)
    page2 = await isolated_repo.list_paginated(skip=2, limit=2)
    assert len(page1) == 2
    assert len(page2) == 2
    assert {s.path for s in page1} & {s.path for s in page2} == set()


async def test_count_returns_total(isolated_repo):
    assert await isolated_repo.count() == 0
    for i in range(3):
        await isolated_repo.create(_sample_skill(path=f"/skills/{i}", name=f"n{i}"))
    assert await isolated_repo.count() == 3


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------


async def test_create_many_inserts_all_in_one_statement(isolated_repo):
    skills = [_sample_skill(path=f"/skills/{i}", name=f"n{i}") for i in range(3)]
    returned = await isolated_repo.create_many(skills)
    assert returned == skills
    assert await isolated_repo.count() == 3


async def test_create_many_empty_list_is_noop(isolated_repo):
    assert await isolated_repo.create_many([]) == []


async def test_update_many_upserts_per_path(isolated_repo):
    existing = _sample_skill(path="/skills/a", name="a", is_enabled=False)
    await isolated_repo.create(existing)

    # /skills/a is updated; /skills/b is upserted (insert path).
    updates: dict[str, dict[str, Any]] = {
        "/skills/a": {"is_enabled": True},
        "/skills/b": {"name": "newcomer", "is_enabled": True, "tags": ["x"]},
    }
    n = await isolated_repo.update_many(updates)
    assert n == 2
    assert await isolated_repo.get_state("/skills/a") is True
    assert await isolated_repo.get_state("/skills/b") is True


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


async def test_get_with_dead_pool_returns_none(monkeypatch):
    """Connection errors on get() are swallowed — returns None."""
    from registry.repositories.postgres import skill_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresSkillRepository()
    assert await repo.get("/skills/x") is None
    assert await repo.count() == 0
