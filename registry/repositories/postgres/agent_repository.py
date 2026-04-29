"""PostgreSQL repository for A2A agent storage.

Mirrors ``registry/repositories/documentdb/agent_repository.py`` and satisfies
the ``AgentRepositoryBase`` ABC at ``registry/repositories/interfaces.py``
lines 183-305.

Storage layout
--------------
Table ``mcp_agents_{namespace}`` (POSTGRES-B-002) keyed by the AgentCard
``path`` (e.g. ``"/agents/research-bot"``). Hot-field columns
``name`` / ``visibility`` are GENERATED ALWAYS from the JSONB body; only
``data`` and ``is_enabled`` are written directly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import asyncpg

from ...schemas.agent_models import AgentCard
from ..interfaces import AgentRepositoryBase
from . import mongo_filter
from .client import get_pool, table_name  # POSTGRES-D pattern

logger = logging.getLogger(__name__)


class PostgresAgentRepository(AgentRepositoryBase):
    """PostgreSQL/JSONB implementation of the A2A agent repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_agents")
        logger.info(
            "Initialized Postgres AgentRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _decode(value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value

    def _row_to_card(self, row: asyncpg.Record) -> AgentCard | None:
        """Hydrate a (id, data) row into an AgentCard. Returns None if the
        stored payload fails Pydantic validation (data-quality issue, not a
        control-flow error — matches documentdb behavior)."""
        doc = self._decode(row["data"])
        doc["path"] = row["id"]
        try:
            return AgentCard(**doc)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping invalid agent document %s: %s", row["id"], exc)
            return None

    # ------------------------------------------------------------------ ABC

    async def load_all(self) -> None:
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                count = await conn.fetchval(sql)
            logger.info("Loaded %s agents from Postgres", count)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error loading agents from Postgres: %s", exc, exc_info=True)

    async def get(self, path: str) -> AgentCard | None:
        sql = f"SELECT id, data FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting agent '%s' from Postgres: %s", path, exc, exc_info=True)
            return None

        if row is None:
            return None
        return self._row_to_card(row)

    async def list_all(self) -> list[AgentCard]:
        sql = f"SELECT id, data FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error listing agents from Postgres: %s", exc, exc_info=True)
            return []
        return [card for row in rows if (card := self._row_to_card(row)) is not None]

    async def list_paginated(
        self,
        skip: int = 0,
        limit: int = 100,
    ) -> list[AgentCard]:
        sql = (
            f"SELECT id, data FROM {self._table_name} "
            "ORDER BY id OFFSET $1 LIMIT $2"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, skip, limit)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error listing paginated agents from Postgres: %s", exc, exc_info=True
            )
            return []
        return [card for row in rows if (card := self._row_to_card(row)) is not None]

    async def create(self, agent: AgentCard) -> AgentCard:
        """Insert a new AgentCard. Raises ``ValueError`` on duplicate path
        (parity with documentdb implementation)."""
        if not agent.path:
            raise ValueError("AgentCard.path is required for create()")
        if not agent.registered_at:
            agent.registered_at = datetime.utcnow()
        if not agent.updated_at:
            agent.updated_at = datetime.utcnow()
        agent.is_enabled = False

        doc = agent.model_dump(mode="json")
        path = doc.pop("path")

        sql = (
            f"INSERT INTO {self._table_name} (id, is_enabled, data) "
            "VALUES ($1, $2, $3::jsonb) ON CONFLICT (id) DO NOTHING RETURNING 1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                inserted = await conn.fetchval(sql, path, False, json.dumps(doc))
        except asyncpg.PostgresError as exc:
            logger.error("Failed to create agent in Postgres: %s", exc, exc_info=True)
            raise ValueError(f"Failed to create agent: {exc}") from exc

        if inserted is None:
            logger.error("Agent path '%s' already exists in Postgres", path)
            raise ValueError(f"Agent path '{path}' already exists")
        logger.info("Created agent '%s' at '%s'", agent.name, path)
        return agent

    async def update(self, path: str, updates: dict[str, Any]) -> AgentCard:
        """Patch-merge ``updates`` onto the stored card and persist."""
        existing = await self.get(path)
        if not existing:
            logger.error("Cannot update agent at '%s': not found", path)
            raise ValueError(f"Agent not found at path: {path}")

        merged = existing.model_dump()
        merged.update(updates)
        merged["updated_at"] = datetime.utcnow()

        try:
            updated = AgentCard(**merged)
        except Exception as exc:
            logger.error("Failed to validate updated agent: %s", exc)
            raise ValueError(f"Invalid agent update: {exc}") from exc

        doc = updated.model_dump(mode="json")
        doc.pop("path", None)

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
            logger.error("Failed to update agent in Postgres: %s", exc, exc_info=True)
            raise ValueError(f"Failed to update agent: {exc}") from exc

        if result.endswith(" 0"):
            raise ValueError(f"Agent at '{path}' not found in Postgres")
        logger.info("Updated agent '%s' (%s)", updated.name, path)
        return updated

    async def delete(self, path: str) -> bool:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to delete agent from Postgres: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Agent at '%s' not found in Postgres", path)
            return False
        logger.info("Deleted agent at '%s'", path)
        return True

    async def get_state(
        self,
        path: str | None = None,
    ) -> dict[str, list[str]] | bool:
        """When ``path`` is None, return a dict bucketing all agents into
        ``{"enabled": [...], "disabled": [...]}`` (parity with documentdb
        implementation lines 199-220)."""
        if path is None:
            sql = f"SELECT id, is_enabled FROM {self._table_name}"
            try:
                async with (await self._pool()).acquire() as conn:
                    rows = await conn.fetch(sql)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error getting agent states: %s", exc, exc_info=True)
                return {"enabled": [], "disabled": []}
            state: dict[str, list[str]] = {"enabled": [], "disabled": []}
            for row in rows:
                bucket = "enabled" if row["is_enabled"] else "disabled"
                state[bucket].append(row["id"])
            return state

        sql = f"SELECT is_enabled FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                value = await conn.fetchval(sql, path)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting state for agent '%s': %s", path, exc, exc_info=True)
            return False
        return bool(value) if value is not None else False

    async def set_state(self, path: str, enabled: bool) -> bool:
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
            logger.error("Failed to set state for agent '%s': %s", path, exc, exc_info=True)
            return False
        if result.endswith(" 0"):
            logger.error("Agent at '%s' not found", path)
            return False
        logger.info("Toggled agent '%s' to %s", path, enabled)
        return True

    async def save_state(self, state: dict[str, list[str]]) -> None:
        """Compatibility no-op (file-repo holdover; matches documentdb impl)."""
        logger.debug(
            "Updated agent state cache: %d enabled, %d disabled",
            len(state.get("enabled", [])), len(state.get("disabled", [])),
        )

    async def count(self) -> int:
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                return int(await conn.fetchval(sql))
        except Exception as exc:  # noqa: BLE001
            logger.error("Error counting agents: %s", exc, exc_info=True)
            return 0

    async def update_field(self, path: str, field: str, value: Any) -> bool:
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
