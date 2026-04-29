"""
registry/repositories/postgres/client.py — singleton asyncpg pool + utilities.

Mirrors `registry/repositories/documentdb/client.py` (singleton motor client at
documentdb/client.py:12-13 and namespace suffix at documentdb/client.py:51-55)
so the Postgres backend slots into the existing factory pattern with minimal
review surface.

This module is the ONLY place that calls `asyncpg.create_pool`. Repositories
must go through `get_pool()` and `_table()`.

Companion design memo: `postgres-D-connection-mgmt.md`.

External deps (add to pyproject.toml):
    asyncpg>=0.29.0
    pgvector>=0.3.0
    # tenacity already present for federation retries

NOTE: This is a skeleton — every function body is the minimum needed to
illustrate the contract. Implementation review follows in Phase B.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import ssl as ssl_module
from typing import Any, AsyncIterator, Optional

import asyncpg
import pgvector.asyncpg
import tenacity

from ...core.config import settings  # registry/core/config.py — see memo §5

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Singleton state — mirrors documentdb/client.py:12-13
# ---------------------------------------------------------------------------

_pool: Optional[asyncpg.Pool] = None
_pool_lock: asyncio.Lock = asyncio.Lock()

# Namespace suffix validator. Must match POSTGRES-B's DDL generator.
_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]{0,30}$")


# ---------------------------------------------------------------------------
# TLS / SSL helper — memo §6
# ---------------------------------------------------------------------------

_VALID_SSL_MODES = {
    "disable", "allow", "prefer", "require", "verify-ca", "verify-full",
}


def _build_ssl(
    mode: str,
    ca_file: Optional[str],
) -> "str | ssl_module.SSLContext | bool":
    """
    Translate a libpq-style ssl mode into an asyncpg-compatible argument.

    For verify-ca/verify-full we build an SSLContext with the CA bundle loaded;
    for the other modes we pass the mode name through. asyncpg accepts both.
    """
    if mode not in _VALID_SSL_MODES:
        raise ValueError(
            f"Invalid POSTGRES_SSL_MODE={mode!r}; "
            f"expected one of {sorted(_VALID_SSL_MODES)}"
        )

    if mode in ("verify-ca", "verify-full"):
        if not ca_file:
            raise ValueError(
                f"POSTGRES_SSL_MODE={mode} requires POSTGRES_SSL_CA_FILE"
            )
        ctx = ssl_module.create_default_context(cafile=ca_file)
        ctx.check_hostname = (mode == "verify-full")
        ctx.verify_mode = ssl_module.CERT_REQUIRED
        return ctx

    return mode  # asyncpg accepts "disable" / "allow" / "prefer" / "require"


# ---------------------------------------------------------------------------
# Per-connection setup — memo §4
# ---------------------------------------------------------------------------

async def _setup_connection(conn: asyncpg.Connection) -> None:
    """
    Run once per physical connection (on creation and on reconnect).

    1. Register pgvector codec so `vector` columns round-trip as numpy arrays.
    2. Apply server-side statement_timeout if configured.
    3. Tag application_name for pg_stat_activity visibility.
    """
    await pgvector.asyncpg.register_vector(conn)

    timeout_ms = settings.postgres_statement_timeout_ms
    if timeout_ms and timeout_ms > 0:
        # SET (not SET LOCAL) — persists for the connection's lifetime.
        await conn.execute(f"SET statement_timeout = {int(timeout_ms)}")

    await conn.execute("SET application_name = 'mcp-gateway-registry'")


# ---------------------------------------------------------------------------
# Pool lifecycle — memo §3
# ---------------------------------------------------------------------------

async def get_pool() -> asyncpg.Pool:
    """
    Lazy-init the singleton asyncpg pool. Safe under concurrent first-callers
    via _pool_lock (asyncpg's create_pool is async and not cheap to do twice).
    """
    global _pool
    if _pool is not None:
        return _pool

    async with _pool_lock:
        if _pool is not None:  # double-checked under lock
            return _pool

        ns = settings.postgres_namespace
        if not _NAMESPACE_RE.match(ns):
            raise ValueError(
                f"Invalid POSTGRES_NAMESPACE={ns!r}; "
                f"must match {_NAMESPACE_RE.pattern}"
            )

        ssl_arg = _build_ssl(
            settings.postgres_ssl_mode,
            settings.postgres_ssl_ca_file,
        )

        # Prefer DSN if provided; else compose from split fields.
        if settings.postgres_dsn:
            dsn = settings.postgres_dsn
            connect_kwargs: dict[str, Any] = {}
        else:
            dsn = None
            connect_kwargs = {
                "host": settings.postgres_host,
                "port": settings.postgres_port,
                "database": settings.postgres_database,
                "user": settings.postgres_username,
                "password": settings.postgres_password,
            }

        logger.info(
            "Initializing Postgres pool (min=%d, max=%d, ssl=%s, ns=%s)",
            settings.postgres_pool_min,
            settings.postgres_pool_max,
            settings.postgres_ssl_mode,
            ns,
        )

        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=settings.postgres_pool_min,
            max_size=settings.postgres_pool_max,
            command_timeout=settings.postgres_command_timeout_s,
            statement_cache_size=settings.postgres_statement_cache_size,
            ssl=ssl_arg,
            init=_setup_connection,
            **connect_kwargs,
        )
        return _pool


async def close_pool() -> None:
    """
    Close the pool and reset the singleton. Idempotent — safe from a FastAPI
    shutdown handler regardless of whether the pool was ever created.
    """
    global _pool
    async with _pool_lock:
        if _pool is None:
            return
        try:
            await _pool.close()
        finally:
            _pool = None
            logger.info("Postgres pool closed")


# ---------------------------------------------------------------------------
# Table-name namespacing — mirrors documentdb/client.py:51-55
# ---------------------------------------------------------------------------

def _table(base_name: str) -> str:
    """
    Apply the multi-tenant namespace suffix.

        _table("mcp_servers") -> "mcp_servers_default"

    Used inside f-strings for SQL composition. NOT for $1-style parameters
    (asyncpg parameters carry values, not identifiers).
    """
    return f"{base_name}_{settings.postgres_namespace}"


# Public alias used by repository modules (POSTGRES-D pattern).
# Tests monkeypatch this name on each repository module.
def table_name(base_name: str) -> str:
    return _table(base_name)


# ---------------------------------------------------------------------------
# Retry policy — memo §7
# ---------------------------------------------------------------------------

postgres_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type((
        asyncpg.PostgresConnectionError,
        asyncio.TimeoutError,
    )),
    wait=tenacity.wait_exponential_jitter(initial=0.1, max=2.0),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
)
"""
Apply at the repository-method level, not at pool.acquire — a retry needs to
re-acquire from the pool too. Do NOT wrap health_check() with this; health
should fail fast.
"""


# ---------------------------------------------------------------------------
# Health probe — memo §8
# ---------------------------------------------------------------------------

_HEALTH_TIMEOUT_S = 2.0


async def health_check() -> dict[str, Any]:
    """
    Lightweight liveness probe. Independent timeout (2 s) so a wedged pool
    surfaces promptly rather than hiding behind command_timeout (60 s).

    Returns the same shape as upstream's mongo health probe.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        version = await asyncio.wait_for(
            conn.fetchval("SELECT version()"),
            timeout=_HEALTH_TIMEOUT_S,
        )
    return {
        "status": "ok",
        "backend": "postgres",
        "version": version,
        "namespace": settings.postgres_namespace,
        "pool": {
            "size": pool.get_size(),
            "idle": pool.get_idle_size(),
            "min": pool.get_min_size(),
            "max": pool.get_max_size(),
        },
    }


# ---------------------------------------------------------------------------
# Advisory lock helper — memo §10
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def advisory_lock(
    conn: asyncpg.Connection,
    key: int,
    *,
    transactional: bool = False,
) -> AsyncIterator[None]:
    """
    Cross-host mutual exclusion via Postgres advisory locks.

    Use this — NOT flock — for "exactly-one across the cluster" coordination
    (migration runner, bulk import, single-leader tasks). flock is reserved
    for single-host shell scripts in the operator runbook (see memo §10).

        async with pool.acquire() as conn:
            async with advisory_lock(conn, key=0xDEADBEEF):
                await _run_migrations(conn)

    `transactional=True` uses pg_advisory_xact_lock — auto-released on
    commit/rollback, only valid inside `conn.transaction()`.
    """
    if transactional:
        await conn.execute("SELECT pg_advisory_xact_lock($1)", key)
        yield
        return

    await conn.execute("SELECT pg_advisory_lock($1)", key)
    try:
        yield
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", key)


# ---------------------------------------------------------------------------
# Convenience: acquire-and-yield a connection (matches motor idiom)
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def acquire() -> AsyncIterator[asyncpg.Connection]:
    """
    Sugar for `async with (await get_pool()).acquire() as conn`.

    Most repository code already destructures this pattern; provide it once
    so the get_pool() call site doesn't bloat every method.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


__all__ = [
    "get_pool",
    "close_pool",
    "acquire",
    "_table",
    "table_name",
    "postgres_retry",
    "health_check",
    "advisory_lock",
]
