"""PostgreSQL repository for FederationConfig storage.

Implements ``FederationConfigRepositoryBase`` (interfaces.py:1053-1107) with
behaviour parity against
``registry/repositories/documentdb/federation_config_repository.py``.

Storage layout
--------------
Per POSTGRES-B-006, the table ``mcp_federation_config_{namespace}`` is a
plain ``(id TEXT PK, data JSONB, created_at, updated_at)`` shape. The PK
column carries the configuration identifier (default: ``"default"``) and
``data`` carries the full Pydantic dump. There are no hot columns and no
secondary indexes — the access path is always ``id = $1``.

Timestamps are owned by the database: ``created_at`` defaults to ``now()``
on INSERT, ``updated_at`` is bumped by the ``mcp_set_updated_at()`` trigger.
``list_configs()`` reads the timestamp columns directly (matching the
DocumentDB shape that exposes ``created_at`` / ``updated_at`` from the
document body).
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

from ...schemas.federation_schema import FederationConfig
from ..interfaces import FederationConfigRepositoryBase
from .client import _table as table_name
from .client import get_pool

logger = logging.getLogger(__name__)


class PostgresFederationConfigRepository(FederationConfigRepositoryBase):
    """PostgreSQL/JSONB implementation of the Federation Config repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_federation_config")
        logger.info(
            "Initialized Postgres FederationConfigRepository with table: %s",
            self._table_name,
        )

    # ------------------------------------------------------------------ pool

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # -------------------------------------------------------------------- ABC

    async def get_config(self, config_id: str = "default") -> FederationConfig | None:
        """Retrieve a federation config by id. Returns None on miss or error.

        Matches documentdb behavior of swallowing read-side errors so a wedged
        DB does not break the federation-discovery surface (callers fall back
        to defaults).
        """
        sql = f"SELECT data FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, config_id)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to get federation config %s: %s", config_id, exc, exc_info=True
            )
            return None

        if row is None:
            logger.info("Federation config not found: %s", config_id)
            return None

        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        try:
            config = FederationConfig(**data)
        except Exception as exc:  # noqa: BLE001 — corrupt row is data-quality
            logger.error(
                "Stored federation config %s failed validation: %s",
                config_id,
                exc,
                exc_info=True,
            )
            return None
        logger.info("Retrieved federation config: %s", config_id)
        return config

    async def save_config(
        self,
        config: FederationConfig,
        config_id: str = "default",
    ) -> FederationConfig:
        """Upsert a federation config row.

        Mirrors documentdb's ``replace_one(..., upsert=True)``: any existing
        row is replaced wholesale; if absent, a new row is inserted.
        Database-owned timestamps are stripped from the JSONB body to keep a
        single source of truth.
        """
        doc = config.model_dump(mode="json")
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
                    await conn.execute(sql, config_id, payload)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to save federation config %s: %s",
                config_id,
                exc,
                exc_info=True,
            )
            raise
        logger.info("Saved federation config: %s", config_id)
        return config

    async def delete_config(self, config_id: str = "default") -> bool:
        """Delete a federation config row. Returns False if absent or on error."""
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, config_id)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to delete federation config %s: %s",
                config_id,
                exc,
                exc_info=True,
            )
            return False
        deleted = _rowcount_from_tag(result) > 0
        if deleted:
            logger.info("Deleted federation config: %s", config_id)
        else:
            logger.warning("Federation config not found for deletion: %s", config_id)
        return deleted

    async def list_configs(self) -> list[dict[str, Any]]:
        """Return ``[{id, created_at, updated_at}, ...]`` summaries.

        Timestamps come from the table columns, not the JSONB body. Casted
        to ISO-8601 strings to match the DocumentDB surface, which returned
        ``datetime``-string values straight from the document.
        """
        sql = f"""
            SELECT id,
                   to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
                   to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
              FROM {self._table_name}
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to list federation configs: %s", exc, exc_info=True)
            return []

        configs = [
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
        logger.info("Listed %d federation configs", len(configs))
        return configs


def _rowcount_from_tag(tag: str) -> int:
    """Parse asyncpg ``execute()`` command-tag (e.g., 'DELETE 1', 'UPDATE 0')."""
    parts = tag.split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0
