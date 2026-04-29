"""PostgreSQL helper for peer sync state (mcp_peer_sync_state table per B-008).

The peer-federation surface is a single ABC
(``PeerFederationRepositoryBase`` at ``interfaces.py:981-1051``) that spans
TWO tables: ``mcp_peers`` (peer config) and ``mcp_peer_sync_state``
(per-peer watermarks). To keep each module focused on a single table —
matching the file layout of ``migrations/postgres/`` — the sync-state
storage lives here as ``PostgresPeerSyncStateStore`` and is composed into
``PostgresPeerFederationRepository`` rather than implementing the ABC
directly. There is no separate ABC for sync state.

The 1:1 relationship between peers and sync-state rows is enforced by the
schema (FK with ``ON DELETE CASCADE`` on ``mcp_peer_sync_state.id``);
``delete_peer`` therefore needs no app-side cascade — Postgres does it
atomically. This is the chief structural improvement over the DocumentDB
impl, which has to delete from two collections in sequence.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

from ...schemas.peer_federation_schema import PeerSyncStatus
from .client import get_pool, table_name

logger = logging.getLogger(__name__)


# DDL kept here so test fixtures can spin the table up without a migration runner.
# Keep in sync with ``migrations/postgres/postgres-B-tables-008-peer_sync_state.sql``.
# Note: the FK to mcp_peers_default is omitted from this snippet because tests
# may exercise the sync-state store in isolation. Production schema has it.
TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id          TEXT PRIMARY KEY,
    data        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

TRIGGER_DDL = """
DROP TRIGGER IF EXISTS {table}_updated_at ON {table};
CREATE TRIGGER {table}_updated_at
    BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


class PostgresPeerSyncStateStore:
    """Thin storage helper for the ``mcp_peer_sync_state_{namespace}`` table.

    Not an ABC implementation by itself — the ABC methods that touch sync
    state (``get_sync_status``, ``update_sync_status``, ``list_sync_statuses``)
    live on ``PostgresPeerFederationRepository`` and delegate here.
    """

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_peer_sync_state")
        logger.info(
            "Initialized Postgres PeerSyncStateStore with table: %s",
            self._table_name,
        )

    @property
    def table_name(self) -> str:
        return self._table_name

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _row_to_status(row: asyncpg.Record) -> PeerSyncStatus | None:
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        if not isinstance(data, dict):
            logger.error(
                "Sync-state row %r has non-dict JSONB body — skipping",
                row.get("id") if hasattr(row, "get") else None,
            )
            return None
        try:
            return PeerSyncStatus(**data)
        except Exception as exc:  # noqa: BLE001 — match documentdb behavior
            peer_id = data.get("peer_id", row["id"]) if isinstance(data, dict) else row["id"]
            logger.error(
                "Failed to parse sync status %s: %s", peer_id, exc, exc_info=True
            )
            return None

    async def get(self, peer_id: str) -> PeerSyncStatus | None:
        """Get sync status for a single peer."""
        sql = f"SELECT id, data FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, peer_id)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to get sync status for %s: %s", peer_id, exc, exc_info=True
            )
            return None
        if row is None:
            logger.debug("Sync status not found for peer: %s", peer_id)
            return None
        return self._row_to_status(row)

    async def upsert(
        self,
        peer_id: str,
        status: PeerSyncStatus,
    ) -> PeerSyncStatus:
        """Upsert sync status for a peer.

        ``updated_at`` is owned by the database (``mcp_set_updated_at`` trigger
        bumps it on UPDATE; column default seeds it on INSERT). The peer_id
        in the JSONB body is forced to match the row id to avoid drift.
        """
        doc = status.model_dump(mode="json")
        doc["peer_id"] = peer_id
        # Drop any caller-supplied updated_at so the DB stays authoritative.
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
                    await conn.execute(sql, peer_id, payload)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to update sync status for %s: %s", peer_id, exc, exc_info=True
            )
            raise ValueError(f"Failed to update sync status: {exc}")

        logger.debug("Updated sync status for peer: %s", peer_id)
        return status

    async def list_all(self) -> list[PeerSyncStatus]:
        """List sync status for every peer."""
        sql = f"SELECT id, data FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error("Failed to list sync statuses: %s", exc, exc_info=True)
            return []

        statuses: list[PeerSyncStatus] = []
        for row in rows:
            parsed = self._row_to_status(row)
            if parsed is not None:
                statuses.append(parsed)
        logger.info("Listed %d sync statuses", len(statuses))
        return statuses

    async def delete(self, peer_id: str) -> bool:
        """Delete sync status for a peer.

        Production schema has ``ON DELETE CASCADE`` on the FK to
        ``mcp_peers_default``, so this is rarely needed by callers — the
        ``DELETE FROM mcp_peers_default`` propagates. It is provided for
        symmetry and for tests that exercise the store in isolation.
        """
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, peer_id)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to delete sync status for %s: %s", peer_id, exc, exc_info=True
            )
            return False
        # asyncpg's execute returns "DELETE <n>".
        return result.endswith(" 1")

    async def count(self) -> int:
        """Return the number of sync-state rows. Used by ``load_all``."""
        sql = f"SELECT COUNT(*)::BIGINT FROM {self._table_name}"
        async with (await self._pool()).acquire() as conn:
            count = await conn.fetchval(sql)
        return int(count or 0)
