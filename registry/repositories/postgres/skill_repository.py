"""PostgreSQL repository for SkillCard storage.

Implements ``SkillRepositoryBase`` (interfaces.py:1109-1237) with the same
behaviour surface as ``registry/repositories/documentdb/skill_repository.py``.

Storage layout
--------------
Per POSTGRES-B-009, the table ``agent_skills_{namespace}`` carries a
JSONB ``data`` column plus four GENERATED hot columns
(``name``, ``visibility``, ``registry_name``, ``owner``) and two writable
hot columns (``is_enabled``, ``tags TEXT[]``). Generated columns are derived
from ``data`` and must NOT be written. Writable hot columns are kept in sync
with their JSONB equivalents on every write so callers see a single
authoritative value regardless of access path.

Timestamp parity follows POSTGRES-F: ``created_at`` defaults on INSERT;
``updated_at`` is bumped by the ``mcp_set_updated_at()`` trigger.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

from ...exceptions import SkillAlreadyExistsError, SkillServiceError
from ...schemas.skill_models import SkillCard
from ..interfaces import SkillRepositoryBase
from .client import _table as table_name  # POSTGRES-D exposes _table; alias for F-style readability
from .client import get_pool

logger = logging.getLogger(__name__)


class PostgresSkillRepository(SkillRepositoryBase):
    """PostgreSQL/JSONB implementation of the Skill repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("agent_skills")
        logger.info(
            "Initialized Postgres SkillRepository with table: %s",
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
    def _to_skill(cls, data: Any) -> SkillCard:
        return SkillCard(**cls._decode(data))

    def _strip_db_owned(self, doc: dict[str, Any]) -> dict[str, Any]:
        # created_at / updated_at are owned by the DB columns + trigger.
        # Strip from JSONB body to keep a single source of truth.
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        return doc

    # -------------------------------------------------------------------- ABC

    async def ensure_indexes(self) -> None:
        """No-op: indexes ship with the migration (POSTGRES-B-009).

        Kept for ABC parity with DocumentDBSkillRepository so callers can
        invoke it unconditionally during cold-start without branching on
        backend type.
        """
        logger.debug(
            "ensure_indexes is a no-op for Postgres (migration-managed): %s",
            self._table_name,
        )

    async def get(self, path: str) -> SkillCard | None:
        sql = f"SELECT data FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error getting skill %s: %s", path, exc, exc_info=True)
            return None

        if row is None:
            return None
        try:
            return self._to_skill(row["data"])
        except Exception as exc:  # noqa: BLE001 — corrupt row is data-quality
            logger.error("Failed to parse skill document %s: %s", path, exc)
            return None

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[SkillCard]:
        sql = f"SELECT data FROM {self._table_name} OFFSET $1 LIMIT $2"
        return await self._fetch_skills(sql, (skip, limit))

    async def list_paginated(self, skip: int = 0, limit: int = 100) -> list[SkillCard]:
        sql = f"SELECT data FROM {self._table_name} ORDER BY id OFFSET $1 LIMIT $2"
        return await self._fetch_skills(sql, (skip, limit))

    async def list_filtered(
        self,
        include_disabled: bool = False,
        tag: str | None = None,
        visibility: str | None = None,
        registry_name: str | None = None,
    ) -> list[SkillCard]:
        clauses: list[str] = []
        params: list[Any] = []
        if not include_disabled:
            clauses.append("is_enabled = TRUE")
        if tag is not None:
            params.append(tag)
            clauses.append(f"tags @> ARRAY[${len(params)}]::text[]")
        if visibility is not None:
            params.append(visibility)
            clauses.append(f"visibility = ${len(params)}")
        if registry_name is not None:
            params.append(registry_name)
            clauses.append(f"registry_name = ${len(params)}")

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT data FROM {self._table_name}{where}"
        return await self._fetch_skills(sql, tuple(params))

    async def _fetch_skills(self, sql: str, params: tuple) -> list[SkillCard]:
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error in list query: %s", exc, exc_info=True)
            return []
        skills: list[SkillCard] = []
        for row in rows:
            try:
                skills.append(self._to_skill(row["data"]))
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to parse skill document: %s", exc)
        return skills

    async def create(self, skill: SkillCard) -> SkillCard:
        doc = self._strip_db_owned(skill.model_dump(mode="json"))
        sql = f"""
            INSERT INTO {self._table_name} (id, is_enabled, tags, data)
            VALUES ($1, $2, $3::text[], $4::jsonb)
        """
        try:
            async with (await self._pool()).acquire() as conn:
                await conn.execute(
                    sql,
                    skill.path,
                    skill.is_enabled,
                    list(skill.tags),
                    json.dumps(doc),
                )
        except asyncpg.UniqueViolationError as exc:
            logger.error("Skill already exists: %s", skill.path)
            raise SkillAlreadyExistsError(skill.name) from exc
        except asyncpg.PostgresError as exc:
            logger.error("Failed to create skill %s: %s", skill.path, exc, exc_info=True)
            raise SkillServiceError(f"Failed to create skill: {exc}") from exc
        logger.info("Created skill: %s", skill.path)
        return skill

    async def update(self, path: str, updates: dict[str, Any]) -> SkillCard | None:
        # Shallow $set-style merge via JSONB concat (data || updates::jsonb).
        # Hot columns (is_enabled, tags) are re-derived from the merged
        # document so they remain in sync with data->is_enabled / data->'tags'.
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
            logger.error("Failed to update skill %s: %s", path, exc, exc_info=True)
            raise SkillServiceError(f"Failed to update skill: {exc}") from exc
        if row is None:
            return None
        try:
            skill = self._to_skill(row["data"])
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse updated skill %s: %s", path, exc)
            return None
        logger.info("Updated skill: %s", path)
        return skill

    async def delete(self, path: str) -> bool:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to delete skill %s: %s", path, exc, exc_info=True)
            return False
        deleted = _rowcount_from_tag(result) > 0
        if deleted:
            logger.info("Deleted skill: %s", path)
        return deleted

    async def get_state(self, path: str) -> bool:
        sql = f"SELECT is_enabled FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to get skill state %s: %s", path, exc)
            return False
        return bool(row and row["is_enabled"])

    async def set_state(self, path: str, enabled: bool) -> bool:
        # Match documentdb behavior: only return True when the value
        # actually changes. Reuses Mongo's `modified_count > 0` semantics
        # via `IS DISTINCT FROM`.
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
            logger.error("Failed to set skill state %s: %s", path, exc)
            return False
        updated = _rowcount_from_tag(result) > 0
        if updated:
            logger.info("Set skill %s enabled=%s", path, enabled)
        return updated

    # --------------------------------------------------------------- batch ops

    async def create_many(self, skills: list[SkillCard]) -> list[SkillCard]:
        """Single-statement bulk INSERT.

        Builds one ``INSERT ... VALUES ($1,$2,...), ($3,$4,...)`` and runs
        it inside a transaction. Strict semantics: any constraint violation
        aborts the whole batch (matches documentdb's ``insert_many`` +
        ``ordered=False``-then-raise behaviour).
        """
        if not skills:
            return []
        placeholders: list[str] = []
        params: list[Any] = []
        for i, s in enumerate(skills):
            doc = self._strip_db_owned(s.model_dump(mode="json"))
            offset = i * 4
            placeholders.append(
                f"(${offset+1}, ${offset+2}, ${offset+3}::text[], ${offset+4}::jsonb)"
            )
            params.extend([s.path, s.is_enabled, list(s.tags), json.dumps(doc)])
        sql = (
            f"INSERT INTO {self._table_name} (id, is_enabled, tags, data) "
            f"VALUES {', '.join(placeholders)}"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(sql, *params)
        except asyncpg.UniqueViolationError as exc:
            logger.error("Duplicate skill in batch: %s", exc)
            raise SkillServiceError(f"Batch create failed: {exc}") from exc
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to create %d skills in batch: %s", len(skills), exc, exc_info=True
            )
            raise SkillServiceError(f"Batch create failed: {exc}") from exc
        logger.info("Created %d skills in batch", len(skills))
        return skills

    async def update_many(self, updates: dict[str, dict[str, Any]]) -> int:
        """Per-key UPSERT (matches documentdb's ``upsert=True`` per-path).

        For each path:
          - If the row exists, shallow-merge the updates into ``data`` and
            re-derive hot columns (same path as ``update()``).
          - If the row does not exist, INSERT a new row carrying the
            partial ``update_data`` as the document body. Hot columns are
            seeded from the partial dict (defaults applied when absent).

        Returns the number of paths that were modified or inserted.
        """
        if not updates:
            return 0

        sql_upd = f"""
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
             WHERE id = $1 AND EXISTS (SELECT 1 FROM merged)
             RETURNING id
        """
        sql_ins = f"""
            INSERT INTO {self._table_name} (id, is_enabled, tags, data)
            VALUES (
                $1,
                COALESCE(($2::jsonb)->>'is_enabled', 'false')::boolean,
                COALESCE(
                    ARRAY(SELECT jsonb_array_elements_text(($2::jsonb)->'tags')),
                    '{{}}'::text[]
                ),
                $2::jsonb
            )
            ON CONFLICT (id) DO NOTHING
        """
        count = 0
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    for path, update_data in updates.items():
                        payload = json.dumps(update_data)
                        row = await conn.fetchrow(sql_upd, path, payload)
                        if row is not None:
                            count += 1
                            continue
                        result = await conn.execute(sql_ins, path, payload)
                        if _rowcount_from_tag(result) > 0:
                            count += 1
        except asyncpg.PostgresError as exc:
            logger.error("Failed batch update_many: %s", exc, exc_info=True)
            raise SkillServiceError(f"Batch update failed: {exc}") from exc
        logger.info("Updated %d skills in batch", count)
        return count

    async def count(self) -> int:
        sql = f"SELECT COUNT(*) AS n FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql)
        except asyncpg.PostgresError as exc:
            logger.error("Error counting skills: %s", exc, exc_info=True)
            return 0
        return int(row["n"]) if row else 0


def _rowcount_from_tag(tag: str) -> int:
    """Parse asyncpg ``execute()`` command-tag (e.g., 'DELETE 1', 'UPDATE 0').

    Returns 0 for unparseable tags so callers treat them as no-op.
    """
    parts = tag.split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0
