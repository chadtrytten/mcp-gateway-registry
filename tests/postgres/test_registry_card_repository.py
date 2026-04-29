"""Integration tests for PostgresRegistryCardRepository.

Ready-to-fork target:
    tests/integration/test_postgres_registry_card_repository.py

Follows the upstream pattern (see ``tests/integration/test_mongodb_connectivity.py``)
of skipping in CI when no DB is available. Two ways to run locally:

1. ``POSTGRES_TEST_DSN=postgresql:///mcp_test pytest …`` against a localhost
   server with ``pgvector`` installed.
2. testcontainers-postgres pattern (preferred when contributors don't have a
   local Postgres). Drop-in fixture sketch is provided at the bottom of this
   file but commented out — testcontainers is a new dep for upstream and
   should land in its own PR per P3 §7.2.
"""

from __future__ import annotations

import json
import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.registry_card_repository import (
    CARD_ID,
    TABLE_DDL,
    TRIGGER_DDL,
    PostgresRegistryCardRepository,
)
from registry.schemas.registry_card import (
    RegistryAuthConfig,
    RegistryCapabilities,
    RegistryCard,
    RegistryContact,
)

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason=(
            "Requires Postgres running — set POSTGRES_TEST_DSN to enable. "
            "Pattern matches tests/integration/test_mongodb_connectivity.py."
        ),
    ),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
async def isolated_repo(monkeypatch):
    """A repo wired to a per-test schema in the test database.

    Uses a randomly-named schema so parallel test runs cannot collide. Each
    schema gets the table + trigger + global ``mcp_set_updated_at()``
    function recreated; tear-down drops the schema CASCADE.
    """
    assert POSTGRES_TEST_DSN  # guarded by pytestmark

    schema = f"test_pgrcr_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".registry_cards_default'
    async with pool.acquire() as conn:
        await conn.execute(TABLE_DDL.format(table=table))
        await conn.execute(TRIGGER_DDL.format(table=table))

    # Patch get_pool() and table_name() so the repo points at our scoped
    # schema. We keep the original module references for restore.
    from registry.repositories.postgres import client as pg_client
    from registry.repositories.postgres import registry_card_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(
        mod, "table_name", lambda base: f'"{schema}".{base}_default'
    )

    repo = PostgresRegistryCardRepository()
    try:
        yield repo
    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _sample_card(name: str = "Test Registry") -> RegistryCard:
    return RegistryCard(
        name=name,
        description="A test registry instance",
        federation_endpoint="https://registry.example.test/api/federation",
        organization_name="Walter Munk Foundation",
        capabilities=RegistryCapabilities(servers=True, agents=True),
        authentication=RegistryAuthConfig(),
        visibility_policy="authenticated",
        contact=RegistryContact(email="ops@example.test"),
        metadata={"region": "us-west-2"},
    )


# ---------------------------------------------------------------------------
# One test per public ABC method, plus the upsert / corruption edge cases.
# ---------------------------------------------------------------------------


async def test_get_returns_none_when_table_empty(isolated_repo):
    """get() on an empty table is a clean None — no exception."""
    assert await isolated_repo.get() is None


async def test_save_inserts_then_get_round_trips_full_payload(isolated_repo):
    """save() persists the Pydantic dump verbatim; get() reconstructs it."""
    card = _sample_card()
    saved = await isolated_repo.save(card)
    assert saved is card  # save returns the input card

    fetched = await isolated_repo.get()
    assert fetched is not None
    assert fetched.id == card.id
    assert fetched.name == card.name
    assert fetched.description == card.description
    assert fetched.organization_name == "Walter Munk Foundation"
    assert fetched.capabilities.servers is True
    assert fetched.metadata == {"region": "us-west-2"}
    # Server-side timestamps populated.
    assert fetched.created_at is not None
    assert fetched.updated_at is not None


async def test_save_upsert_preserves_created_at_and_advances_updated_at(
    isolated_repo,
):
    """A second save() must not overwrite created_at; updated_at must advance."""
    card = _sample_card(name="V1")
    await isolated_repo.save(card)
    first = await isolated_repo.get()
    assert first is not None
    original_created_at = first.created_at
    original_updated_at = first.updated_at

    # Mutate and re-save.
    card.name = "V2"
    card.description = "Updated description"
    await isolated_repo.save(card)
    second = await isolated_repo.get()

    assert second is not None
    assert second.name == "V2"
    assert second.description == "Updated description"
    # created_at preserved by DB column default + ON CONFLICT not touching it.
    assert second.created_at == original_created_at
    # updated_at bumped by mcp_set_updated_at() trigger.
    assert second.updated_at >= original_updated_at


async def test_exists_false_then_true(isolated_repo):
    """exists() flips False → True after the first save."""
    assert await isolated_repo.exists() is False
    await isolated_repo.save(_sample_card())
    assert await isolated_repo.exists() is True


async def test_get_with_corrupt_row_returns_none(isolated_repo, monkeypatch):
    """A row whose JSONB fails Pydantic validation returns None, not a crash."""
    # Inject a row that is not a valid RegistryCard.
    pool = await isolated_repo._pool()
    async with pool.acquire() as conn:
        # Use a literal ``$1::jsonb`` parameter so we exercise the same
        # encode path as save().
        await conn.execute(
            f"INSERT INTO {isolated_repo._table_name} (id, data) "
            f"VALUES ($1, $2::jsonb)",
            CARD_ID,
            json.dumps({"name": "missing required federation_endpoint"}),
        )
    assert await isolated_repo.get() is None


async def test_save_raises_on_dead_pool(monkeypatch):
    """Connection errors on save() bubble up to the caller."""
    from registry.repositories.postgres import registry_card_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover — only the call shape matters
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresRegistryCardRepository()

    with pytest.raises(asyncpg.PostgresConnectionError):
        await repo.save(_sample_card())


async def test_get_with_dead_pool_returns_none(monkeypatch):
    """Connection errors on get() are swallowed — returns None."""
    from registry.repositories.postgres import registry_card_repository as mod

    class _DeadPool:
        def acquire(self):  # pragma: no cover
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresRegistryCardRepository()

    assert await repo.get() is None
    assert await repo.exists() is False


# ---------------------------------------------------------------------------
# testcontainers fixture sketch (kept commented per P3 §7.2 Path A)
# ---------------------------------------------------------------------------
# from testcontainers.postgres import PostgresContainer
#
# @pytest.fixture(scope="session")
# def pg_container():
#     with PostgresContainer("pgvector/pgvector:pg16") as pg:
#         yield pg
#
# @pytest.fixture
# async def isolated_repo_tc(pg_container, monkeypatch):
#     dsn = pg_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
#     ...  # same body as isolated_repo using ``dsn`` instead of POSTGRES_TEST_DSN
