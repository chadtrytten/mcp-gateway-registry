"""PostgreSQL repository for MCP server registry storage.

Mirrors the DocumentDB implementation at
``registry/repositories/documentdb/server_repository.py`` and satisfies the
``ServerRepositoryBase`` ABC defined at ``registry/repositories/interfaces.py``
lines 26-181.

Storage layout
--------------
Table ``mcp_servers_{namespace}`` (POSTGRES-B-001) keyed by ``id`` (server
path, e.g. ``"/context7"`` or ``"/context7:v2"``). The full Pydantic dump
lives in ``data JSONB``. Hot-field columns (``server_name``, ``source``,
``status``) are GENERATED ALWAYS — only ``data`` is written.
``is_enabled`` is materialized (not generated) and is updated atomically
with the JSONB body so set_state() stays a single statement.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import asyncpg

from ..interfaces import ServerRepositoryBase
from . import mongo_filter
from .client import get_pool, table_name  # POSTGRES-D pattern

logger = logging.getLogger(__name__)


class PostgresServerRepository(ServerRepositoryBase):
    """PostgreSQL/JSONB implementation of the MCP server repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_servers")
        logger.info(
            "Initialized Postgres ServerRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _decode(value: Any) -> Any:
        """asyncpg returns JSONB as a Python object when the codec is
        registered; fall back to json.loads if a string slipped through."""
        if isinstance(value, str):
            return json.loads(value)
        return value

    def _row_to_doc(self, row: asyncpg.Record) -> dict[str, Any]:
        """Hydrate a (id, data) row into the documentdb-shape dict
        (``path`` injected, mirrors documentdb/server_repository.py:90-92)."""
        doc = self._decode(row["data"])
        doc["path"] = row["id"]
        return doc

    # ------------------------------------------------------------------ ABC

    async def load_all(self) -> None:
        """Touch the table to confirm reachability + log total count."""
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                count = await conn.fetchval(sql)
            logger.info("Loaded %s servers from Postgres", count)
        except Exception as exc:  # noqa: BLE001 — match documentdb behavior
            logger.error("Error loading servers from Postgres: %s", exc, exc_info=True)

    async def get(self, path: str) -> dict[str, Any] | None:
        """Get server by path. Falls back to the alternate trailing-slash
        form (parity with documentdb/server_repository.py:55-65)."""
        alternate = path.rstrip("/") if path.endswith("/") else path + "/"
        sql = f"SELECT id, data FROM {self._table_name} WHERE id = $1 OR id = $2 LIMIT 1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path, alternate)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting server '%s' from Postgres: %s", path, exc, exc_info=True)
            return None

        if row is None:
            logger.debug("Server not found at '%s'", path)
            return None
        return self._row_to_doc(row)

    async def list_all(self) -> dict[str, dict[str, Any]]:
        """List all servers as ``{path: doc}``."""
        sql = f"SELECT id, data FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error listing servers from Postgres: %s", exc, exc_info=True)
            return {}

        servers = {row["id"]: self._row_to_doc(row) for row in rows}
        logger.info("Retrieved %d servers from Postgres", len(servers))
        return servers

    async def list_paginated(
        self,
        skip: int = 0,
        limit: int = 100,
    ) -> dict[str, dict[str, Any]]:
        sql = (
            f"SELECT id, data FROM {self._table_name} "
            "ORDER BY id OFFSET $1 LIMIT $2"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, skip, limit)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error listing paginated servers from Postgres: %s", exc, exc_info=True
            )
            return {}

        return {row["id"]: self._row_to_doc(row) for row in rows}

    async def list_by_source(self, source: str) -> dict[str, dict[str, Any]]:
        # ``source`` is a GENERATED column on the table — indexed btree.
        sql = f"SELECT id, data FROM {self._table_name} WHERE source = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, source)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error listing servers by source '%s' from Postgres: %s",
                source, exc, exc_info=True,
            )
            return {}

        return {row["id"]: self._row_to_doc(row) for row in rows}

    async def create(self, server_info: dict[str, Any]) -> bool:
        """Insert a new server row. Returns False if the path already exists
        (mirrors documentdb DuplicateKeyError handling)."""
        if "path" not in server_info:
            logger.error("create() requires server_info['path']")
            return False

        doc = {**server_info}
        path = doc.pop("path")
        is_enabled = bool(doc.get("is_enabled", False))
        doc["is_enabled"] = is_enabled
        now_iso = datetime.utcnow().isoformat()
        doc["registered_at"] = now_iso
        doc["updated_at"] = now_iso

        sql = (
            f"INSERT INTO {self._table_name} (id, is_enabled, data) "
            "VALUES ($1, $2, $3::jsonb) ON CONFLICT (id) DO NOTHING RETURNING 1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                inserted = await conn.fetchval(sql, path, is_enabled, json.dumps(doc))
        except asyncpg.PostgresError as exc:
            logger.error("Failed to create server in Postgres: %s", exc, exc_info=True)
            return False

        if inserted is None:
            logger.error("Server path '%s' already exists in Postgres", path)
            return False
        logger.info(
            "Created server '%s' at '%s'",
            server_info.get("server_name", "unknown"), path,
        )
        return True

    async def update(self, path: str, server_info: dict[str, Any]) -> bool:
        """Replace the JSONB body and is_enabled column atomically. The
        ``mcp_set_updated_at`` trigger advances ``updated_at`` automatically."""
        doc = {**server_info}
        doc.pop("path", None)
        doc["updated_at"] = datetime.utcnow().isoformat()
        # Caller may or may not have set is_enabled in the dump; if absent,
        # COALESCE keeps the existing column value.
        sql = (
            f"UPDATE {self._table_name} "
            "SET data = $2::jsonb, "
            "    is_enabled = COALESCE(($2::jsonb->>'is_enabled')::bool, is_enabled) "
            "WHERE id = $1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path, json.dumps(doc))
        except asyncpg.PostgresError as exc:
            logger.error("Failed to update server in Postgres: %s", exc, exc_info=True)
            return False

        # asyncpg returns "UPDATE <n>"; <n>=0 means no row matched.
        if result.endswith(" 0"):
            logger.error("Server at '%s' not found in Postgres", path)
            return False
        logger.info("Updated server at '%s'", path)
        return True

    async def delete(self, path: str) -> bool:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to delete server from Postgres: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Server at '%s' not found in Postgres", path)
            return False
        logger.info("Deleted server at '%s'", path)
        return True

    async def delete_with_versions(self, path: str) -> int:
        """Delete the active row plus any rows whose id starts with
        ``path + ':'`` (versioned children)."""
        sql = (
            f"DELETE FROM {self._table_name} "
            "WHERE id = $1 OR id LIKE $1 || ':%' RETURNING 1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to delete server and versions from Postgres: %s", exc, exc_info=True
            )
            return 0
        deleted = len(rows)
        if deleted == 0:
            logger.error("No documents found for server at '%s'", path)
        else:
            logger.info("Deleted %d document(s) for server at '%s'", deleted, path)
        return deleted

    async def get_state(self, path: str) -> bool:
        sql = f"SELECT is_enabled FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                value = await conn.fetchval(sql, path)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting state for '%s': %s", path, exc, exc_info=True)
            return False
        return bool(value) if value is not None else False

    async def set_state(self, path: str, enabled: bool) -> bool:
        """Update both the ``is_enabled`` column and the JSONB body's
        ``is_enabled`` field atomically."""
        sql = (
            f"UPDATE {self._table_name} "
            "SET is_enabled = $2, "
            "    data = jsonb_set(data, '{is_enabled}', to_jsonb($2::bool)) "
            "WHERE id = $1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path, enabled)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to set state for '%s': %s", path, exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Server at '%s' not found", path)
            return False
        logger.info("Toggled server '%s' to %s", path, enabled)
        return True

    async def count(self) -> int:
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                return int(await conn.fetchval(sql))
        except Exception as exc:  # noqa: BLE001
            logger.error("Error counting servers: %s", exc, exc_info=True)
            return 0

    async def update_field(self, path: str, field: str, value: Any) -> bool:
        """Atomically set/unset a JSONB field. Nested dot-paths are
        translated into a JSONB path array (``a.b`` → ``{a,b}``)."""
        parts = field.split(".")
        for part in parts:
            if not part or not part.replace("_", "").isalnum():
                logger.error("update_field: unsafe field name %r", field)
                return False

        if value is None:
            sql = f"UPDATE {self._table_name} SET data = data #- $2::text[] WHERE id = $1"
            params: tuple[Any, ...] = (path, parts)
        else:
            sql = (
                f"UPDATE {self._table_name} "
                "SET data = jsonb_set(data, $2::text[], $3::jsonb, true) WHERE id = $1"
            )
            params = (path, parts, json.dumps(value))

        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("update_field failed for '%s'.%s: %s", path, field, exc, exc_info=True)
            return False
        return not result.endswith(" 0")

    async def find_with_filter(
        self, filter_dict: dict[str, Any],
    ) -> dict[str, dict]:
        """Translate the Mongo-style filter via mongo_filter and run it."""
        try:
            where_sql, params = mongo_filter.translate(
                filter_dict, id_column="id", data_column="data"
            )
        except mongo_filter.TranslationError as exc:
            logger.error("Unsupported find_with_filter %r: %s", filter_dict, exc)
            return {}

        sql = f"SELECT id, data FROM {self._table_name} WHERE {where_sql}"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("find_with_filter query failed: %s", exc, exc_info=True)
            return {}
        return {row["id"]: self._decode(row["data"]) for row in rows}
