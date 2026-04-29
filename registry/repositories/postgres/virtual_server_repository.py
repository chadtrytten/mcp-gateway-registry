"""PostgreSQL repository for VirtualServerConfig storage.

Implements ``VirtualServerRepositoryBase`` (interfaces.py:1336-1458) with
behaviour parity against ``registry/repositories/documentdb/virtual_server_repository.py``.

Storage layout
--------------
Per POSTGRES-B-011, the table ``virtual_servers_{namespace}`` carries a
JSONB ``data`` column plus one GENERATED hot column (``server_name``) and
two writable hot columns (``is_enabled``, ``tags TEXT[]``). The repository
keeps the writable columns in sync with their JSONB equivalents on every
write.

Timestamp parity follows POSTGRES-F: ``created_at`` defaults on INSERT;
``updated_at`` is bumped by the ``mcp_set_updated_at()`` trigger.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

from ...exceptions import VirtualServerAlreadyExistsError, VirtualServerServiceError
from ...schemas.virtual_server_models import VirtualServerConfig
from ..interfaces import VirtualServerRepositoryBase
from .client import _table as table_name
from .client import get_pool

logger = logging.getLogger(__name__)


class PostgresVirtualServerRepository(VirtualServerRepositoryBase):
    """PostgreSQL/JSONB implementation of the Virtual Server repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("virtual_servers")
        logger.info(
            "Initialized Postgres VirtualServerRepository with table: %s",
            self._table_name,
        )

    # ------------------------------------------------------------------ pool

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # ----------------------------------------------------------------- helpers

    @staticmethod
    def _decode(data: Any) -> dict[str, Any]:
        if isinstance(data, str):
            return json.loads(data)
        return dict(data)

    @classmethod
    def _to_config(cls, data: Any) -> VirtualServerConfig:
        return VirtualServerConfig(**cls._decode(data))

    def _strip_db_owned(self, doc: dict[str, Any]) -> dict[str, Any]:
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        return doc

    # -------------------------------------------------------------------- ABC

    async def ensure_indexes(self) -> None:
        """No-op: indexes ship with the migration (POSTGRES-B-011)."""
        logger.debug(
            "ensure_indexes is a no-op for Postgres (migration-managed): %s",
            self._table_name,
        )

    async def get(self, path: str) -> VirtualServerConfig | None:
        sql = f"SELECT data FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Postgres error getting virtual server %s: %s", path, exc, exc_info=True
            )
            return None

        if row is None:
            return None
        try:
            return self._to_config(row["data"])
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse virtual server document %s: %s", path, exc)
            return None

    async def list_all(self) -> list[VirtualServerConfig]:
        sql = f"SELECT data FROM {self._table_name}"
        return await self._fetch_configs(sql, ())

    async def list_enabled(self) -> list[VirtualServerConfig]:
        sql = f"SELECT data FROM {self._table_name} WHERE is_enabled = TRUE"
        return await self._fetch_configs(sql, ())

    async def _fetch_configs(self, sql: str, params: tuple) -> list[VirtualServerConfig]:
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error in list query: %s", exc, exc_info=True)
            return []
        configs: list[VirtualServerConfig] = []
        for row in rows:
            try:
                configs.append(self._to_config(row["data"]))
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to parse virtual server document: %s", exc)
        return configs

    async def create(self, config: VirtualServerConfig) -> VirtualServerConfig:
        doc = self._strip_db_owned(config.model_dump(mode="json"))
        sql = f"""
            INSERT INTO {self._table_name} (id, is_enabled, tags, data)
            VALUES ($1, $2, $3::text[], $4::jsonb)
        """
        try:
            async with (await self._pool()).acquire() as conn:
                await conn.execute(
                    sql,
                    config.path,
                    config.is_enabled,
                    list(config.tags),
                    json.dumps(doc),
                )
        except asyncpg.UniqueViolationError as exc:
            logger.error("Virtual server already exists: %s", config.path)
            raise VirtualServerAlreadyExistsError(config.path) from exc
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to create virtual server %s: %s", config.path, exc, exc_info=True
            )
            raise VirtualServerServiceError(
                f"Failed to create virtual server: {exc}"
            ) from exc
        logger.info("Created virtual server: %s", config.path)
        return config

    async def update(
        self, path: str, updates: dict[str, Any]
    ) -> VirtualServerConfig | None:
        sql = f"""
            WITH merged AS (
                SELECT data || $2::jsonb AS data
                FROM {self._table_name}
                WHERE id = $1
            )
            UPDATE {self._table_name}
               SET data = (SELECT data FROM merged),
                   is_enabled = COALESCE(
                       ((SELECT data FROM merged)->>'is_enabled')::boolean,
                       is_enabled
                   ),
                   tags = COALESCE(
                       (SELECT ARRAY(
                           SELECT jsonb_array_elements_text(
                               (SELECT data FROM merged)->'tags'
                           )
                       )),
                       tags
                   )
             WHERE id = $1
               AND EXISTS (SELECT 1 FROM merged)
             RETURNING data
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path, json.dumps(updates))
        except asyncpg.PostgresError as exc:
            logger.error("Failed to update virtual server %s: %s", path, exc, exc_info=True)
            raise VirtualServerServiceError(
                f"Failed to update virtual server: {exc}"
            ) from exc
        if row is None:
            return None
        try:
            config = self._to_config(row["data"])
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse updated virtual server %s: %s", path, exc)
            return None
        logger.info("Updated virtual server: %s", path)
        return config

    async def delete(self, path: str) -> bool:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to delete virtual server %s: %s", path, exc, exc_info=True)
            return False
        deleted = _rowcount_from_tag(result) > 0
        if deleted:
            logger.info("Deleted virtual server: %s", path)
        return deleted

    async def get_state(self, path: str) -> bool:
        sql = f"SELECT is_enabled FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to get virtual server state %s: %s", path, exc)
            return False
        return bool(row and row["is_enabled"])

    async def set_state(self, path: str, enabled: bool) -> bool:
        # Match documentdb behavior: only return True when the value actually
        # changes (Mongo's modified_count > 0 semantics).
        sql = f"""
            UPDATE {self._table_name}
               SET is_enabled = $2,
                   data = jsonb_set(data, '{{is_enabled}}', to_jsonb($2::boolean))
             WHERE id = $1 AND is_enabled IS DISTINCT FROM $2
        """
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path, enabled)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to set virtual server state %s: %s", path, exc)
            return False
        updated = _rowcount_from_tag(result) > 0
        if updated:
            logger.info("Set virtual server %s enabled=%s", path, enabled)
        return updated


def _rowcount_from_tag(tag: str) -> int:
    """Parse asyncpg ``execute()`` command-tag (e.g., 'DELETE 1', 'UPDATE 0')."""
    parts = tag.split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0
