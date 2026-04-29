"""PostgreSQL repository for backend MCP session storage.

Mirrors ``registry/repositories/documentdb/backend_session_repository.py`` and
satisfies ``BackendSessionRepositoryBase`` (interfaces.py:1239-1334).

Storage layout
--------------
Single table ``backend_sessions_{namespace}`` with two row kinds:

* ``kind='backend'`` — id = '<client_session_id>:<backend_key>'
* ``kind='client'``  — id = 'client:<client_session_id>'

Both share ``last_used_at TIMESTAMPTZ`` which is the TTL anchor. Mongo uses
``expireAfterSeconds=3600`` on a TTL index; Postgres has no native TTL, so the
schema (postgres-B-tables-012) wires two fallback paths:

1. **pg_cron** — installed by the migration prelude when the extension is
   available; runs ``DELETE … WHERE last_used_at < now() - interval '1 hour'``
   every 5 minutes.
2. **In-app sweeper** — :func:`start_ttl_sweeper` launches an asyncio task that
   runs the same DELETE every 5 minutes when pg_cron is unavailable. The repo
   probes ``pg_extension`` once at first use to decide.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

from ..interfaces import BackendSessionRepositoryBase
from .client import get_pool, table_name

logger = logging.getLogger(__name__)


# Mongo parity: expireAfterSeconds=3600.
SESSION_TTL_SECONDS: int = 3600

# Sweeper cadence (matches pg_cron schedule '*/5 * * * *').
SWEEPER_INTERVAL_SECONDS: int = 300


def _make_backend_session_id(client_session_id: str, backend_key: str) -> str:
    return f"{client_session_id}:{backend_key}"


def _make_client_session_id(client_session_id: str) -> str:
    return f"client:{client_session_id}"


# ---------------------------------------------------------------------------
# TTL sweeper (Option B fallback when pg_cron is unavailable)
# ---------------------------------------------------------------------------

_sweeper_task: Optional[asyncio.Task] = None
_sweeper_lock = asyncio.Lock()


async def _has_pg_cron_job(conn: asyncpg.Connection, table: str) -> bool:
    """Return True iff pg_cron has the named job already scheduled."""
    row = await conn.fetchrow(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'"
    )
    if row is None:
        return False
    try:
        row = await conn.fetchrow(
            "SELECT 1 FROM cron.job WHERE jobname = $1",
            f"{table}_ttl",
        )
        return row is not None
    except asyncpg.PostgresError:
        # cron.job not visible to this role → assume not scheduled.
        return False


async def _sweeper_loop(table: str) -> None:
    while True:
        try:
            async with (await get_pool()).acquire() as conn:
                deleted = await conn.fetchval(
                    f"WITH d AS (DELETE FROM {table} "
                    f"WHERE last_used_at < now() - interval '{SESSION_TTL_SECONDS} seconds' "
                    f"RETURNING 1) SELECT count(*) FROM d"
                )
            if deleted:
                logger.info("backend_sessions sweeper deleted %d expired rows", deleted)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — sweeper must never die on transient errors
            logger.exception("backend_sessions sweeper iteration failed")
        await asyncio.sleep(SWEEPER_INTERVAL_SECONDS)


async def start_ttl_sweeper(table: str) -> bool:
    """Start the in-app TTL sweeper iff pg_cron isn't already handling it.

    Returns True if a sweeper task was started, False if pg_cron is active.
    Idempotent: safe to call multiple times from the FastAPI startup event.
    """
    global _sweeper_task
    async with _sweeper_lock:
        if _sweeper_task is not None and not _sweeper_task.done():
            return True
        async with (await get_pool()).acquire() as conn:
            if await _has_pg_cron_job(conn, table):
                logger.info("pg_cron job %s_ttl is active; skipping in-app sweeper", table)
                return False
        _sweeper_task = asyncio.create_task(
            _sweeper_loop(table), name=f"{table}_ttl_sweeper"
        )
        logger.info(
            "Started in-app TTL sweeper for %s (every %ds, TTL=%ds)",
            table,
            SWEEPER_INTERVAL_SECONDS,
            SESSION_TTL_SECONDS,
        )
        return True


async def stop_ttl_sweeper() -> None:
    """Cancel the in-app sweeper (FastAPI shutdown hook)."""
    global _sweeper_task
    async with _sweeper_lock:
        if _sweeper_task is None:
            return
        _sweeper_task.cancel()
        try:
            await _sweeper_task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("backend_sessions sweeper raised on shutdown")
        finally:
            _sweeper_task = None


class PostgresBackendSessionRepository(BackendSessionRepositoryBase):
    """PostgreSQL/JSONB implementation of backend session storage.

    Behaviour parity with ``DocumentDBBackendSessionRepository``:

    * ``get_backend_session`` and ``validate_client_session`` perform an atomic
      read-and-bump of ``last_used_at`` via ``UPDATE … RETURNING``.
    * ``store_backend_session`` upserts on the composite id.
    * ``ensure_indexes`` is the place we hook the TTL sweeper start; the
      DocumentDB impl creates the TTL index here.
    """

    def __init__(self) -> None:
        self._table_name: str = table_name("backend_sessions")
        self._sweeper_started: bool = False
        logger.info(
            "Initialized Postgres BackendSessionRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # ------------------------------------------------------------------ ABC

    async def ensure_indexes(self) -> None:
        """Postgres equivalent of DocumentDB's TTL+index creation.

        Indexes are created by the migration runner (postgres-B-tables-012);
        this hook starts the in-app TTL sweeper iff pg_cron isn't installed,
        matching the Mongo TTL behaviour.
        """
        if self._sweeper_started:
            return
        try:
            await start_ttl_sweeper(self._table_name)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to start TTL sweeper: %s", exc, exc_info=True)
            return
        self._sweeper_started = True

    async def get_backend_session(
        self,
        client_session_id: str,
        backend_key: str,
    ) -> str | None:
        """Atomic read-and-bump.

        Equivalent to Mongo's ``find_one_and_update`` with ``$set`` on
        ``last_used_at``. ``UPDATE … RETURNING`` keeps it single-roundtrip.
        """
        doc_id = _make_backend_session_id(client_session_id, backend_key)
        sql = f"""
            UPDATE {self._table_name}
               SET last_used_at = now()
             WHERE id = $1
             RETURNING backend_session_id
        """
        try:
            async with (await self._pool()).acquire() as conn:
                value = await conn.fetchval(sql, doc_id)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error reading backend session: %s", exc)
            return None
        except Exception as exc:  # noqa: BLE001 — match documentdb impl behaviour
            logger.error("Error getting backend session: %s", exc, exc_info=True)
            return None

        return value

    async def store_backend_session(
        self,
        client_session_id: str,
        backend_key: str,
        backend_session_id: str,
        user_id: str,
        virtual_server_path: str,
    ) -> None:
        """Upsert a backend-session binding row."""
        doc_id = _make_backend_session_id(client_session_id, backend_key)
        sql = f"""
            INSERT INTO {self._table_name} (
                id, kind, client_session_id, backend_key,
                backend_session_id, user_id, virtual_server_path,
                created_at, last_used_at
            )
            VALUES ($1, 'backend', $2, $3, $4, $5, $6, now(), now())
            ON CONFLICT (id) DO UPDATE SET
                client_session_id   = EXCLUDED.client_session_id,
                backend_key         = EXCLUDED.backend_key,
                backend_session_id  = EXCLUDED.backend_session_id,
                user_id             = EXCLUDED.user_id,
                virtual_server_path = EXCLUDED.virtual_server_path,
                last_used_at        = now()
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        sql,
                        doc_id,
                        client_session_id,
                        backend_key,
                        backend_session_id,
                        user_id,
                        virtual_server_path,
                    )
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error storing backend session: %s", exc)
            raise
        except Exception as exc:
            logger.error("Error storing backend session: %s", exc, exc_info=True)
            raise

        logger.debug("Stored backend session: %s -> %s", doc_id, backend_session_id)

    async def delete_backend_session(
        self,
        client_session_id: str,
        backend_key: str,
    ) -> None:
        """Delete a stale backend session binding row."""
        doc_id = _make_backend_session_id(client_session_id, backend_key)
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, doc_id)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error deleting backend session: %s", exc)
            raise
        except Exception as exc:
            logger.error("Error deleting backend session: %s", exc, exc_info=True)
            raise

        # asyncpg returns "DELETE n" on execute().
        if result.endswith(" 0"):
            return
        logger.debug("Deleted backend session: %s", doc_id)

    async def create_client_session(
        self,
        client_session_id: str,
        user_id: str,
        virtual_server_path: str,
    ) -> None:
        """Insert a client-session row used by ``validate_client_session``.

        Mongo uses ``insert_one`` (no upsert). Match that — duplicate inserts
        raise ``UniqueViolationError`` and propagate to the caller.
        """
        doc_id = _make_client_session_id(client_session_id)
        sql = f"""
            INSERT INTO {self._table_name} (
                id, kind, client_session_id,
                user_id, virtual_server_path,
                created_at, last_used_at
            )
            VALUES ($1, 'client', $2, $3, $4, now(), now())
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        sql,
                        doc_id,
                        client_session_id,
                        user_id,
                        virtual_server_path,
                    )
        except asyncpg.UniqueViolationError:
            # Mirror Mongo behaviour: surface duplicate-key as the same kind of error.
            logger.error("Client session already exists: %s", client_session_id)
            raise
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error creating client session: %s", exc)
            raise
        except Exception as exc:
            logger.error("Error creating client session: %s", exc, exc_info=True)
            raise

        logger.info(
            "Created client session: %s for user=%s path=%s",
            client_session_id,
            user_id,
            virtual_server_path,
        )

    async def validate_client_session(
        self,
        client_session_id: str,
    ) -> bool:
        """Atomic exists-and-bump."""
        doc_id = _make_client_session_id(client_session_id)
        sql = f"""
            UPDATE {self._table_name}
               SET last_used_at = now()
             WHERE id = $1
             RETURNING 1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchval(sql, doc_id)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error validating client session: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Error validating client session: %s", exc, exc_info=True)
            return False

        return row is not None
