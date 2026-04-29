"""PostgreSQL repository for Registry Card storage.

Reference implementation, ready-to-fork into:
    registry/repositories/postgres/registry_card_repository.py

Mirrors the DocumentDB implementation at
``registry/repositories/documentdb/registry_card_repository.py`` and satisfies
the ``RegistryCardRepositoryBase`` ABC defined at
``registry/repositories/interfaces.py:1460-1480`` (3 abstract methods:
``get()``, ``save(card)``, ``exists()``).

Storage layout
--------------
Single-row table ``registry_cards_{namespace}`` keyed by the constant
``id = 'default'``. Full Pydantic dump lives in ``data JSONB``. Timestamp
columns (``created_at``, ``updated_at``) are owned by the database — a default
on INSERT and the ``mcp_set_updated_at()`` trigger on UPDATE — and are
synthesised back into the JSONB body on read so callers see the same surface
as the DocumentDB impl.

Imports below assume the ready-to-fork target package; the leading dots
resolve when this file lands at ``registry/repositories/postgres/``.
"""

from __future__ import annotations

import json
import logging

import asyncpg

from ...schemas.registry_card import RegistryCard
from ..interfaces import RegistryCardRepositoryBase
from .client import get_pool, table_name  # POSTGRES-D pattern

logger = logging.getLogger(__name__)


# Singleton primary key for the registry-card row.
CARD_ID = "default"


# DDL canonically lives in postgres/migrations/001_initial.sql; this string is
# included so test fixtures and ad-hoc smoke checks can spin the table up
# without dragging in the migration runner. Keep in sync with POSTGRES-B.
TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id          TEXT PRIMARY KEY,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS {table}_data_gin
    ON {table} USING GIN (data jsonb_path_ops);
"""

# Trigger DDL is split out because PostgreSQL has no
# ``CREATE TRIGGER IF NOT EXISTS``; tests may need to drop+recreate.
TRIGGER_DDL = """
DROP TRIGGER IF EXISTS {table}_updated_at ON {table};
CREATE TRIGGER {table}_updated_at
    BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


class PostgresRegistryCardRepository(RegistryCardRepositoryBase):
    """PostgreSQL/JSONB implementation of the Registry Card repository.

    The class is intentionally tiny — RegistryCard is a singleton — but it
    follows the same lazy-pool + try/except shape that every other Postgres
    repository in this module uses, so it doubles as the canonical
    proof-of-concept for the rest of the backend.
    """

    def __init__(self) -> None:
        self._table_name: str = table_name("registry_cards")
        logger.info(
            "Initialized Postgres RegistryCardRepository with table: %s",
            self._table_name,
        )

    # ------------------------------------------------------------------ pool

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # ------------------------------------------------------------------ ABC

    async def get(self) -> RegistryCard | None:
        """Retrieve the singleton Registry Card. Return None if absent.

        Behavior parity with DocumentDB impl: connection / unexpected errors
        are logged and swallowed (return None). This is appropriate for a
        federation-discovery surface where DB hiccups should not 500.
        """
        # Synthesise timestamps from columns into the JSONB body so the
        # rehydrated RegistryCard carries server-authoritative timestamps.
        sql = f"""
            SELECT jsonb_set(
                       jsonb_set(data, '{{created_at}}', to_jsonb(created_at)),
                       '{{updated_at}}', to_jsonb(updated_at)
                   ) AS data
            FROM {self._table_name}
            WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, CARD_ID)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error fetching registry card: %s", exc)
            return None
        except Exception as exc:  # noqa: BLE001 — match documentdb impl behavior
            logger.error("Error getting registry card: %s", exc, exc_info=True)
            return None

        if row is None:
            logger.debug("No registry card found in database")
            return None

        # asyncpg returns JSONB as a Python dict when the JSONB codec is
        # registered on the connection (POSTGRES-D's ``_setup_connection``).
        # Fall back to json.loads if the codec is missing — keeps this module
        # robust against pool-init drift.
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)

        try:
            card = RegistryCard(**data)
        except Exception as exc:  # noqa: BLE001 — corrupt row is data-quality, not control flow
            logger.error("Stored registry card failed validation: %s", exc, exc_info=True)
            return None

        logger.info("Retrieved registry card from Postgres")
        return card

    async def save(self, card: RegistryCard) -> RegistryCard:
        """Upsert the singleton Registry Card.

        Row timestamps are owned by the database: ``created_at`` defaults to
        ``now()`` on INSERT and is left alone on UPDATE; ``updated_at`` is
        bumped by the ``mcp_set_updated_at()`` trigger on UPDATE. We strip
        any timestamps the caller passed in the Pydantic dump to keep the DB
        as the source of truth.
        """
        doc = card.model_dump(mode="json")
        # DB columns own these — drop from JSONB body to avoid divergence.
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        payload = json.dumps(doc)

        sql = f"""
            INSERT INTO {self._table_name} (id, data)
            VALUES ($1, $2::jsonb)
            ON CONFLICT (id) DO UPDATE
                SET data = EXCLUDED.data
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(sql, CARD_ID, payload)
        except asyncpg.IntegrityConstraintViolationError as exc:
            # Should not occur for a single-row upsert, but log+raise so callers
            # see schema mismatches loudly rather than as silent corruption.
            logger.error(
                "Constraint violation saving registry card: %s", exc, exc_info=True
            )
            raise
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            # Write-path: surface connection errors so the caller can retry.
            # (Reads swallow them; writes don't.)
            logger.error("Postgres connection error saving registry card: %s", exc)
            raise
        except Exception as exc:
            logger.error("Error saving registry card: %s", exc, exc_info=True)
            raise

        logger.info("Saved registry card to Postgres")
        return card

    async def exists(self) -> bool:
        """Check whether a Registry Card row exists.

        Returns False on connection/unexpected errors — matches documentdb
        impl, which is the right call for a probe used by health endpoints.
        """
        sql = f"SELECT 1 FROM {self._table_name} WHERE id = $1 LIMIT 1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, CARD_ID)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error checking registry card: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Error checking registry card existence: %s", exc, exc_info=True)
            return False

        result = row is not None
        logger.debug("Registry card exists check: %s", result)
        return result
