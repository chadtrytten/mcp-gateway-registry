"""PostgreSQL repository for skill security scan results.

Mirrors the DocumentDB implementation at
``registry/repositories/documentdb/skill_security_scan_repository.py`` and
satisfies the ``SkillSecurityScanRepositoryBase`` ABC defined at
``registry/repositories/interfaces.py:764-854``.

Storage layout
--------------
Append-only log table ``mcp_skill_security_scans_{namespace}`` with a
``BIGSERIAL`` surrogate primary key (one row per scan). A compound index on
``(skill_path, scan_timestamp DESC)`` serves the dominant ``get_latest()``
read path. Severity counts are denormalized columns (matching the
``mcp_security_scans`` table layout) so dashboards can aggregate without
unwrapping JSONB.

Hot columns are written explicitly at insert time — they are NOT
``GENERATED ALWAYS`` for this table (see ``postgres-B-tables-005``). The
full scan payload is also kept verbatim in the ``data`` JSONB column.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from ..interfaces import SkillSecurityScanRepositoryBase
from .client import get_pool, table_name

logger = logging.getLogger(__name__)


# DDL kept here so test fixtures can spin the table up without a migration runner.
# Keep in sync with ``migrations/postgres/postgres-B-tables-005-skill_security_scans.sql``.
TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id                      BIGSERIAL PRIMARY KEY,
    skill_path              TEXT NOT NULL,
    scan_status             TEXT NOT NULL,
    scan_timestamp          TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_vulnerabilities   INTEGER NOT NULL DEFAULT 0,
    critical_count          INTEGER NOT NULL DEFAULT 0,
    high_count              INTEGER NOT NULL DEFAULT 0,
    medium_count            INTEGER NOT NULL DEFAULT 0,
    low_count               INTEGER NOT NULL DEFAULT 0,
    data                    JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS {table}_skill_ts
    ON {table} (skill_path, scan_timestamp DESC);
CREATE INDEX IF NOT EXISTS {table}_status
    ON {table} (scan_status);
CREATE INDEX IF NOT EXISTS {table}_data_gin
    ON {table} USING GIN (data jsonb_path_ops);
"""


def _coerce_scan_timestamp(value: Any) -> datetime:
    """Coerce caller-supplied scan_timestamp into an aware datetime.

    Accepts ISO-8601 strings (with or without ``Z``), naive/aware datetimes,
    and falls back to ``now()`` if the value is missing or unparseable.
    Naive datetimes are interpreted as UTC, matching documentdb behavior.
    """
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            # ``datetime.fromisoformat`` handles ``+00:00`` but not ``Z`` until 3.11.
            cleaned = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(cleaned)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            logger.warning("Unparseable scan_timestamp %r — defaulting to now()", value)
            return datetime.now(timezone.utc)
    logger.warning("Non-datetime scan_timestamp %r — defaulting to now()", value)
    return datetime.now(timezone.utc)


def _intify(value: Any) -> int:
    """Coerce ``value`` into a non-negative int, defaulting to 0."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return 0
    return n if n >= 0 else 0


class PostgresSkillSecurityScanRepository(SkillSecurityScanRepositoryBase):
    """PostgreSQL/JSONB implementation of the skill security scan repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_skill_security_scans")
        logger.info(
            "Initialized Postgres SkillSecurityScanRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        if not isinstance(data, dict):
            data = {}
        # Synthesise authoritative columns into the returned dict so callers
        # see DB-owned fields rather than whatever was in the JSONB body.
        data["skill_path"] = row["skill_path"]
        data["scan_status"] = row["scan_status"]
        data["scan_timestamp"] = row["scan_timestamp"].isoformat()
        data.setdefault("total_vulnerabilities", row["total_vulnerabilities"])
        data.setdefault("critical_count", row["critical_count"])
        data.setdefault("high_count", row["high_count"])
        data.setdefault("medium_count", row["medium_count"])
        data.setdefault("low_count", row["low_count"])
        return data

    # -------------------------------------------------------------- ABC

    async def load_all(self) -> None:
        """Verify connectivity / log row count. Mirrors DocumentDB no-op."""
        sql = f"SELECT COUNT(*)::BIGINT FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                count = await conn.fetchval(sql)
            logger.info(
                "Loaded skill security scans from Postgres: %s rows", count
            )
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Error loading skill security scans from Postgres: %s",
                exc,
                exc_info=True,
            )

    async def get(
        self,
        skill_path: str,
    ) -> dict[str, Any] | None:
        """Get latest security scan for a skill — alias for ``get_latest``."""
        return await self.get_latest(skill_path)

    async def list_all(self) -> list[dict[str, Any]]:
        """List all skill security scan results, newest first."""
        sql = f"""
            SELECT skill_path, scan_status, scan_timestamp,
                   total_vulnerabilities, critical_count, high_count,
                   medium_count, low_count, data
            FROM {self._table_name}
            ORDER BY scan_timestamp DESC
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Error listing skill security scans from Postgres: %s",
                exc,
                exc_info=True,
            )
            return []
        return [self._row_to_dict(r) for r in rows]

    async def create(
        self,
        scan_result: dict[str, Any],
    ) -> bool:
        """Append a new skill security scan result.

        ``scan_result`` MUST contain ``skill_path``. Other hot fields default
        to safe values when missing — matches DocumentDB behavior, which
        inserts whatever the caller passes.
        """
        skill_path = scan_result.get("skill_path")
        if not skill_path:
            logger.error("Scan result must contain 'skill_path' field")
            return False

        scan_status = scan_result.get("scan_status") or "pending"
        scan_ts = _coerce_scan_timestamp(scan_result.get("scan_timestamp"))

        # Stamp scan_timestamp into the payload so the JSONB body and the
        # column agree on a single canonical value.
        doc = dict(scan_result)
        doc["scan_timestamp"] = scan_ts.isoformat()

        sql = f"""
            INSERT INTO {self._table_name} (
                skill_path, scan_status, scan_timestamp,
                total_vulnerabilities, critical_count, high_count,
                medium_count, low_count, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        sql,
                        skill_path,
                        scan_status,
                        scan_ts,
                        _intify(scan_result.get("total_vulnerabilities")),
                        _intify(scan_result.get("critical_count")),
                        _intify(scan_result.get("high_count")),
                        _intify(scan_result.get("medium_count")),
                        _intify(scan_result.get("low_count")),
                        json.dumps(doc),
                    )
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to index skill security scan in Postgres: %s",
                exc,
                exc_info=True,
            )
            return False

        logger.info("Indexed skill security scan for %s in Postgres", skill_path)
        return True

    async def get_latest(
        self,
        skill_path: str,
    ) -> dict[str, Any] | None:
        """Get the most recent scan for a skill_path."""
        sql = f"""
            SELECT skill_path, scan_status, scan_timestamp,
                   total_vulnerabilities, critical_count, high_count,
                   medium_count, low_count, data
            FROM {self._table_name}
            WHERE skill_path = $1
            ORDER BY scan_timestamp DESC
            LIMIT 1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, skill_path)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to get latest skill scan from Postgres: %s",
                exc,
                exc_info=True,
            )
            return None
        if row is None:
            return None
        return self._row_to_dict(row)

    async def query_by_status(
        self,
        status: str,
    ) -> list[dict[str, Any]]:
        """Query scan results by ``scan_status``, newest first."""
        sql = f"""
            SELECT skill_path, scan_status, scan_timestamp,
                   total_vulnerabilities, critical_count, high_count,
                   medium_count, low_count, data
            FROM {self._table_name}
            WHERE scan_status = $1
            ORDER BY scan_timestamp DESC
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, status)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to query skill scans by status from Postgres: %s",
                exc,
                exc_info=True,
            )
            return []
        return [self._row_to_dict(r) for r in rows]
