"""PostgreSQL repository for security scan results storage.

Mirrors ``registry/repositories/documentdb/security_scan_repository.py`` and
satisfies ``SecurityScanRepositoryBase`` (interfaces.py:672-762).

Storage layout
--------------
Table ``mcp_security_scans_{namespace}`` (POSTGRES-B-004) — append-only
log keyed by ``BIGSERIAL id``. ``server_path`` + ``scan_timestamp DESC`` is
indexed for ``get_latest()``. Severity counts are denormalized into columns
at insert time so dashboard aggregations don't have to peek into JSONB.
The full scan payload (including ``vulnerabilities`` array) lives in
``data JSONB``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import asyncpg

from ..interfaces import SecurityScanRepositoryBase
from .client import get_pool, table_name  # POSTGRES-D pattern

logger = logging.getLogger(__name__)


_SEVERITIES = ("critical", "high", "medium", "low")


def _compute_vuln_counts(vulnerabilities: list[dict[str, Any]]) -> dict[str, int]:
    counts = {sev: 0 for sev in _SEVERITIES}
    for vuln in vulnerabilities:
        sev = (vuln.get("severity") or "").lower()
        if sev in counts:
            counts[sev] += 1
    return counts


class PostgresSecurityScanRepository(SecurityScanRepositoryBase):
    """PostgreSQL/JSONB implementation of the security scan repository."""

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_security_scans")
        logger.info(
            "Initialized Postgres SecurityScanRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _decode(value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value

    # ------------------------------------------------------------------ ABC

    async def load_all(self) -> None:
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                count = await conn.fetchval(sql)
            logger.info("Loaded %s security scan results from Postgres", count)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error loading security scans from Postgres: %s", exc, exc_info=True)

    async def get(self, server_path: str) -> dict[str, Any] | None:
        """Latest scan for a server path. Delegates to ``get_latest`` so
        the callsite shape matches documentdb."""
        return await self.get_latest(server_path)

    async def list_all(self) -> list[dict[str, Any]]:
        sql = f"SELECT data FROM {self._table_name} ORDER BY scan_timestamp DESC"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error listing security scans from Postgres: %s", exc, exc_info=True)
            return []
        return [self._decode(row["data"]) for row in rows]

    async def create(self, scan_result: dict[str, Any]) -> bool:
        """Insert a new scan row. Accepts ``server_path`` or ``agent_path``
        (the latter is mirrored into ``server_path`` for parity with
        documentdb behavior at lines 75-77)."""
        try:
            doc = {**scan_result}
            path = doc.get("server_path") or doc.get("agent_path")
            if not path:
                logger.error(
                    "Scan result must contain either 'server_path' or 'agent_path' field"
                )
                return False
            if "agent_path" in doc and "server_path" not in doc:
                doc["server_path"] = doc["agent_path"]

            scan_status = doc.get("scan_status", "")

            ts_value = doc.get("scan_timestamp")
            if ts_value is None:
                ts_dt = datetime.utcnow()
                doc["scan_timestamp"] = ts_dt.isoformat()
            elif isinstance(ts_value, datetime):
                ts_dt = ts_value
                doc["scan_timestamp"] = ts_value.isoformat()
            else:
                # Accept ISO strings; let the DB column do the parse via $::timestamptz.
                ts_dt = ts_value  # asyncpg can also accept the str via cast.

            vulns = doc.get("vulnerabilities")
            if isinstance(vulns, list):
                counts = _compute_vuln_counts(vulns)
                doc["total_vulnerabilities"] = len(vulns)
                doc["critical_count"] = counts["critical"]
                doc["high_count"] = counts["high"]
                doc["medium_count"] = counts["medium"]
                doc["low_count"] = counts["low"]
                col_total = len(vulns)
                col_crit, col_high, col_med, col_low = (
                    counts["critical"], counts["high"], counts["medium"], counts["low"]
                )
            else:
                col_total = doc.get("total_vulnerabilities", 0) or 0
                col_crit = doc.get("critical_count", 0) or 0
                col_high = doc.get("high_count", 0) or 0
                col_med = doc.get("medium_count", 0) or 0
                col_low = doc.get("low_count", 0) or 0

            sql = (
                f"INSERT INTO {self._table_name} "
                "(server_path, scan_status, scan_timestamp, "
                " total_vulnerabilities, critical_count, high_count, "
                " medium_count, low_count, data) "
                "VALUES ($1, $2, $3::timestamptz, $4, $5, $6, $7, $8, $9::jsonb)"
            )
            async with (await self._pool()).acquire() as conn:
                await conn.execute(
                    sql,
                    path,
                    scan_status,
                    ts_dt if isinstance(ts_dt, datetime) else doc["scan_timestamp"],
                    col_total, col_crit, col_high, col_med, col_low,
                    json.dumps(doc),
                )
            logger.info("Indexed security scan for %s in Postgres", path)
            return True
        except asyncpg.PostgresError as exc:
            logger.error("Failed to index security scan in Postgres: %s", exc, exc_info=True)
            return False
        except Exception as exc:  # noqa: BLE001 — match documentdb broad-catch
            logger.error("Failed to index security scan in Postgres: %s", exc, exc_info=True)
            return False

    async def get_latest(self, server_path: str) -> dict[str, Any] | None:
        """Newest scan for ``server_path``. Tries both with-slash and
        without-slash variants (parity with documentdb impl lines 113-127)."""
        path_no_slash = server_path.rstrip("/")
        path_with_slash = path_no_slash + "/"
        sql = (
            f"SELECT data FROM {self._table_name} "
            "WHERE server_path = $1 OR server_path = $2 "
            "ORDER BY scan_timestamp DESC LIMIT 1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, path_no_slash, path_with_slash)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to get latest scan from Postgres: %s", exc, exc_info=True)
            return None

        if row is None:
            return None
        return self._decode(row["data"])

    async def query_by_status(self, status: str) -> list[dict[str, Any]]:
        sql = (
            f"SELECT data FROM {self._table_name} "
            "WHERE scan_status = $1 ORDER BY scan_timestamp DESC"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, status)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to query scans by status from Postgres: %s", exc, exc_info=True)
            return []
        return [self._decode(row["data"]) for row in rows]
