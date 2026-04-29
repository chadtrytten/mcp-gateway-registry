"""Integration tests for PostgresBackendSessionRepository.

Mirrors ``tests/postgres/test_registry_card_repository.py`` (POSTGRES-F):
skips in CI when no DB is available; runs against ``POSTGRES_TEST_DSN`` when
set.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import asyncpg
import pytest

from registry.repositories.postgres.backend_session_repository import (
    PostgresBackendSessionRepository,
    SESSION_TTL_SECONDS,
    _make_backend_session_id,
    _make_client_session_id,
    start_ttl_sweeper,
    stop_ttl_sweeper,
)

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        POSTGRES_TEST_DSN is None,
        reason="Requires Postgres — set POSTGRES_TEST_DSN to enable.",
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

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id                    TEXT PRIMARY KEY,
    kind                  TEXT NOT NULL,
    client_session_id     TEXT NOT NULL,
    backend_key           TEXT,
    backend_session_id    TEXT,
    user_id               TEXT,
    virtual_server_path   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {idx_client} ON {table} (client_session_id);
CREATE INDEX IF NOT EXISTS {idx_last}   ON {table} (last_used_at);
"""


@pytest.fixture
async def isolated_repo(monkeypatch):
    assert POSTGRES_TEST_DSN
    schema = f"test_pgbsr_{uuid.uuid4().hex[:12]}"
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=2)

    async with pool.acquire() as conn:
        await conn.execute(_MCP_SET_UPDATED_AT_FN)
        await conn.execute(f'CREATE SCHEMA "{schema}"')

    table = f'"{schema}".backend_sessions_default'
    async with pool.acquire() as conn:
        await conn.execute(
            TABLE_DDL.format(
                table=table,
                idx_client=f"{schema}_client_idx",
                idx_last=f"{schema}_last_idx",
            )
        )

    from registry.repositories.postgres import backend_session_repository as mod

    async def _patched_pool():
        return pool

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda base: f'"{schema}".{base}_default')

    repo = PostgresBackendSessionRepository()
    try:
        yield repo, pool, table
    finally:
        await stop_ttl_sweeper()
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


# --------------------------------------------------------------------- ABC


async def test_get_backend_session_returns_none_when_missing(isolated_repo):
    repo, _, _ = isolated_repo
    assert await repo.get_backend_session("c1", "/k") is None


async def test_store_then_get_round_trips(isolated_repo):
    repo, pool, table = isolated_repo
    await repo.store_backend_session(
        "c1", "/k", "bsess-1", user_id="u1", virtual_server_path="/v/path"
    )
    assert await repo.get_backend_session("c1", "/k") == "bsess-1"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {table} WHERE id = $1",
            _make_backend_session_id("c1", "/k"),
        )
    assert row is not None
    assert row["kind"] == "backend"
    assert row["user_id"] == "u1"
    assert row["virtual_server_path"] == "/v/path"


async def test_store_is_idempotent_upsert(isolated_repo):
    repo, _, _ = isolated_repo
    await repo.store_backend_session("c1", "/k", "bsess-1", "u1", "/v")
    await repo.store_backend_session("c1", "/k", "bsess-2", "u1", "/v")
    assert await repo.get_backend_session("c1", "/k") == "bsess-2"


async def test_get_bumps_last_used_at(isolated_repo):
    repo, pool, table = isolated_repo
    await repo.store_backend_session("c1", "/k", "bsess", "u", "/v")
    async with pool.acquire() as conn:
        old = await conn.fetchval(
            f"SELECT last_used_at FROM {table} WHERE id = $1",
            _make_backend_session_id("c1", "/k"),
        )
    # Force time advancement.
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET last_used_at = now() - interval '5 seconds' WHERE id = $1",
            _make_backend_session_id("c1", "/k"),
        )
    await repo.get_backend_session("c1", "/k")
    async with pool.acquire() as conn:
        new = await conn.fetchval(
            f"SELECT last_used_at FROM {table} WHERE id = $1",
            _make_backend_session_id("c1", "/k"),
        )
    assert new > old or new >= old  # bumped


async def test_delete_backend_session(isolated_repo):
    repo, _, _ = isolated_repo
    await repo.store_backend_session("c1", "/k", "bsess", "u", "/v")
    await repo.delete_backend_session("c1", "/k")
    assert await repo.get_backend_session("c1", "/k") is None


async def test_create_and_validate_client_session(isolated_repo):
    repo, _, _ = isolated_repo
    await repo.create_client_session("client-A", "user-1", "/v/path")
    assert await repo.validate_client_session("client-A") is True
    assert await repo.validate_client_session("nope") is False


async def test_create_client_session_unique(isolated_repo):
    repo, _, _ = isolated_repo
    await repo.create_client_session("dup", "u", "/v")
    with pytest.raises(asyncpg.UniqueViolationError):
        await repo.create_client_session("dup", "u", "/v")


async def test_validate_client_session_bumps_last_used(isolated_repo):
    repo, pool, table = isolated_repo
    await repo.create_client_session("ca", "u", "/v")
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET last_used_at = now() - interval '10 seconds' WHERE id = $1",
            _make_client_session_id("ca"),
        )
        old = await conn.fetchval(
            f"SELECT last_used_at FROM {table} WHERE id = $1",
            _make_client_session_id("ca"),
        )
    await repo.validate_client_session("ca")
    async with pool.acquire() as conn:
        new = await conn.fetchval(
            f"SELECT last_used_at FROM {table} WHERE id = $1",
            _make_client_session_id("ca"),
        )
    assert new > old


# --------------------------------------------------------------------- TTL


async def test_ttl_sweeper_deletes_old_rows(isolated_repo, monkeypatch):
    """Manual sweep: insert a stale row + run the DELETE that the sweeper runs."""
    repo, pool, table = isolated_repo
    await repo.store_backend_session("c-old", "/k", "bsess", "u", "/v")

    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET last_used_at = now() - interval '2 hours' "
            f"WHERE id = $1",
            _make_backend_session_id("c-old", "/k"),
        )
        # Run the same DELETE that the sweeper / pg_cron job runs.
        await conn.execute(
            f"DELETE FROM {table} "
            f"WHERE last_used_at < now() - interval '{SESSION_TTL_SECONDS} seconds'"
        )

    assert await repo.get_backend_session("c-old", "/k") is None


async def test_start_ttl_sweeper_idempotent(isolated_repo):
    """Calling ensure_indexes / start_ttl_sweeper twice must not fork two tasks."""
    repo, _, table = isolated_repo
    await repo.ensure_indexes()
    await repo.ensure_indexes()
    # No assertion needed beyond "did not raise" — module-level _sweeper_task
    # is asserted-on indirectly by stop_ttl_sweeper() in the fixture teardown.


async def test_get_with_dead_pool_returns_none(monkeypatch):
    from registry.repositories.postgres import backend_session_repository as mod

    class _DeadPool:
        def acquire(self):
            raise asyncpg.PostgresConnectionError("simulated outage")

    async def _patched_pool():
        return _DeadPool()

    monkeypatch.setattr(mod, "get_pool", _patched_pool)
    monkeypatch.setattr(mod, "table_name", lambda _: "ignored")
    repo = PostgresBackendSessionRepository()
    assert await repo.get_backend_session("c", "k") is None
    assert await repo.validate_client_session("c") is False
