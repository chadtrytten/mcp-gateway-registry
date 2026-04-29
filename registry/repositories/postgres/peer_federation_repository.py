"""PostgreSQL repository for peer federation configuration storage.

Mirrors the DocumentDB implementation at
``registry/repositories/documentdb/peer_federation_repository.py`` and
satisfies the ``PeerFederationRepositoryBase`` ABC at
``registry/repositories/interfaces.py:981-1051``.

Storage layout
--------------
Two tables, both per-namespace:

* ``mcp_peers_{namespace}`` — peer config. Keyed by ``peer_id`` (text PK).
  ``enabled`` is a materialized BOOLEAN column (not GENERATED) so the
  federation scheduler can index-scan active peers without unwrapping
  JSONB. Body lives in ``data JSONB``. Federation tokens are encrypted
  in-place before INSERT/UPDATE and decrypted on read, matching
  ``DocumentDBPeerFederationRepository``.

* ``mcp_peer_sync_state_{namespace}`` — per-peer watermarks/state. Owned
  by ``PostgresPeerSyncStateStore`` (see ``peer_sync_state_repository.py``)
  and composed in here. The FK with ``ON DELETE CASCADE`` means
  ``delete_peer`` is a single statement instead of the two-step delete
  the documentdb impl performs.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ...schemas.peer_federation_schema import PeerRegistryConfig, PeerSyncStatus
from ...utils.federation_encryption import (
    decrypt_token_in_peer_dict,
    encrypt_token_in_peer_dict,
)
from ..interfaces import PeerFederationRepositoryBase
from .client import get_pool, table_name
from .peer_sync_state_repository import PostgresPeerSyncStateStore

logger = logging.getLogger(__name__)


# DDL for the peers table — sync-state DDL lives in
# ``peer_sync_state_repository.TABLE_DDL``. Keep in sync with
# ``migrations/postgres/postgres-B-tables-007-peers.sql``.
TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id          TEXT PRIMARY KEY,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS {table}_enabled
    ON {table} (enabled);
"""

TRIGGER_DDL = """
DROP TRIGGER IF EXISTS {table}_updated_at ON {table};
CREATE TRIGGER {table}_updated_at
    BEFORE UPDATE ON {table}
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
"""


def _peer_dict_for_storage(config: PeerRegistryConfig) -> dict[str, Any]:
    """Serialize a peer for storage: dump to dict, then encrypt token in-place."""
    doc = config.model_dump(mode="json")
    encrypt_token_in_peer_dict(doc)
    return doc


def _peer_dict_from_storage(doc: dict[str, Any]) -> dict[str, Any]:
    """Deserialize a stored peer dict: decrypt token in-place. Mutates doc."""
    decrypt_token_in_peer_dict(doc)
    return doc


class PostgresPeerFederationRepository(PeerFederationRepositoryBase):
    """PostgreSQL implementation of the peer federation repository.

    Composes ``PostgresPeerSyncStateStore`` for sync-state CRUD so each
    file stays focused on a single table. The ABC remains a single class.
    """

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_peers")
        self._sync_state = PostgresPeerSyncStateStore()
        logger.info(
            "Initialized Postgres PeerFederationRepository with tables: %s, %s",
            self._table_name,
            self._sync_state.table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    @staticmethod
    def _row_to_peer(row: asyncpg.Record) -> PeerRegistryConfig | None:
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        if not isinstance(data, dict):
            logger.error("Peer row %r has non-dict JSONB body — skipping", row["id"])
            return None
        # Authoritative fields from columns: id and DB-owned timestamps.
        data["peer_id"] = row["id"]
        # ``enabled`` lives both as a column and inside ``data``; the column
        # is the source of truth (it's what the index serves).
        data["enabled"] = row["enabled"]
        if row["created_at"] is not None:
            data["created_at"] = row["created_at"].isoformat()
        if row["updated_at"] is not None:
            data["updated_at"] = row["updated_at"].isoformat()
        _peer_dict_from_storage(data)
        try:
            return PeerRegistryConfig(**data)
        except Exception as exc:  # noqa: BLE001 — match documentdb behavior
            logger.error(
                "Failed to parse peer config %s: %s",
                data.get("peer_id", row["id"]),
                exc,
                exc_info=True,
            )
            return None

    # -------------------------------------------------------------- ABC

    async def load_all(self) -> None:
        """Verify connectivity and log peer / sync-state counts."""
        peers_sql = f"SELECT COUNT(*)::BIGINT FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                peer_count = await conn.fetchval(peers_sql)
            sync_count = await self._sync_state.count()
            logger.info(
                "Loaded peer federation data: %s peers, %s sync statuses",
                peer_count,
                sync_count,
            )
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to load peer federation data: %s", exc, exc_info=True
            )
            raise

    async def get_peer(
        self,
        peer_id: str,
    ) -> PeerRegistryConfig | None:
        """Get peer configuration by ID."""
        sql = f"""
            SELECT id, enabled, data, created_at, updated_at
            FROM {self._table_name}
            WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, peer_id)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error("Failed to get peer %s: %s", peer_id, exc, exc_info=True)
            return None
        if row is None:
            logger.debug("Peer not found: %s", peer_id)
            return None
        return self._row_to_peer(row)

    async def list_peers(
        self,
        enabled: bool | None = None,
    ) -> list[PeerRegistryConfig]:
        """List all peers, optionally filtered by enabled-ness."""
        if enabled is None:
            sql = f"""
                SELECT id, enabled, data, created_at, updated_at
                FROM {self._table_name}
            """
            args: tuple[Any, ...] = ()
        else:
            sql = f"""
                SELECT id, enabled, data, created_at, updated_at
                FROM {self._table_name}
                WHERE enabled = $1
            """
            args = (enabled,)
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *args)
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error("Failed to list peers: %s", exc, exc_info=True)
            return []

        peers: list[PeerRegistryConfig] = []
        for row in rows:
            parsed = self._row_to_peer(row)
            if parsed is not None:
                peers.append(parsed)
        logger.info("Listed %d peers (enabled=%s)", len(peers), enabled)
        return peers

    async def create_peer(
        self,
        config: PeerRegistryConfig,
    ) -> PeerRegistryConfig:
        """Create a new peer + initial sync status row.

        The peers→sync_state INSERT pair is wrapped in a transaction so a
        crash midway leaves no half-state. (The ON CONFLICT clause makes
        the peers INSERT collision-safe; the FK on sync_state means we
        MUST insert peers first.)
        """
        peer_id = config.peer_id
        now = datetime.now(UTC)
        config.created_at = now
        config.updated_at = now

        doc = _peer_dict_for_storage(config)
        # Strip duplicated columns; DB owns these.
        doc.pop("created_at", None)
        doc.pop("updated_at", None)
        payload = json.dumps(doc)

        peer_sql = f"""
            INSERT INTO {self._table_name} (id, enabled, data)
            VALUES ($1, $2, $3::jsonb)
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    try:
                        await conn.execute(peer_sql, peer_id, config.enabled, payload)
                    except asyncpg.UniqueViolationError:
                        raise ValueError(f"Peer ID '{peer_id}' already exists")
                    # Seed initial sync status inside the same transaction.
                    initial_status = PeerSyncStatus(peer_id=peer_id)
                    sync_payload = json.dumps(initial_status.model_dump(mode="json"))
                    await conn.execute(
                        f"INSERT INTO {self._sync_state.table_name} (id, data) "
                        f"VALUES ($1, $2::jsonb)",
                        peer_id,
                        sync_payload,
                    )
        except ValueError:
            raise
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to create peer %s: %s", peer_id, exc, exc_info=True
            )
            raise ValueError(f"Failed to create peer: {exc}")

        logger.info("Created peer: %s (%s)", peer_id, config.name)
        return config

    async def update_peer(
        self,
        peer_id: str,
        updates: dict[str, Any],
    ) -> PeerRegistryConfig:
        """Merge ``updates`` into the existing peer config and persist."""
        sql_get = f"""
            SELECT id, enabled, data, created_at, updated_at
            FROM {self._table_name}
            WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(sql_get, peer_id)
                    if row is None:
                        raise ValueError(f"Peer not found: {peer_id}")

                    existing = row["data"]
                    if isinstance(existing, str):
                        existing = json.loads(existing)
                    if not isinstance(existing, dict):
                        existing = {}
                    # Decrypt before merging so callers passing federation_token
                    # in ``updates`` overlay cleanly without losing the
                    # ciphertext field. Issue #561 in documentdb impl.
                    _peer_dict_from_storage(existing)

                    existing.update(updates)
                    existing["peer_id"] = peer_id
                    # ``updated_at`` is column-owned; drop from JSONB body.
                    existing.pop("updated_at", None)
                    existing.pop("created_at", None)

                    try:
                        updated_peer = PeerRegistryConfig(**existing)
                    except Exception as exc:
                        raise ValueError(f"Invalid peer update: {exc}")

                    storage_dict = _peer_dict_for_storage(updated_peer)
                    storage_dict.pop("created_at", None)
                    storage_dict.pop("updated_at", None)
                    payload = json.dumps(storage_dict)

                    update_sql = f"""
                        UPDATE {self._table_name}
                        SET data = $1::jsonb, enabled = $2
                        WHERE id = $3
                    """
                    await conn.execute(
                        update_sql, payload, updated_peer.enabled, peer_id
                    )
        except ValueError:
            raise
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to update peer %s: %s", peer_id, exc, exc_info=True
            )
            raise ValueError(f"Failed to update peer: {exc}")

        logger.info("Updated peer: %s", peer_id)
        return updated_peer

    async def delete_peer(
        self,
        peer_id: str,
    ) -> bool:
        """Delete a peer config. The FK ``ON DELETE CASCADE`` removes sync state."""
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                # Belt-and-braces: also delete sync state explicitly in case
                # the test schema lacks the FK CASCADE (some test fixtures
                # spin up sync_state without the FK to keep the table
                # standalone-runnable). DELETE on a missing row is a no-op.
                result = await conn.execute(sql, peer_id)
                if not result.endswith(" 1"):
                    raise ValueError(f"Peer not found: {peer_id}")
                # In production the CASCADE handles this; in standalone test
                # schemas the explicit delete keeps the rows in lockstep.
                await self._sync_state.delete(peer_id)
        except ValueError:
            raise
        except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
            logger.error(
                "Failed to delete peer %s: %s", peer_id, exc, exc_info=True
            )
            raise ValueError(f"Failed to delete peer: {exc}")

        logger.info("Deleted peer and sync status: %s", peer_id)
        return True

    # -------------------------------------------------------- sync-state

    async def get_sync_status(
        self,
        peer_id: str,
    ) -> PeerSyncStatus | None:
        return await self._sync_state.get(peer_id)

    async def update_sync_status(
        self,
        peer_id: str,
        status: PeerSyncStatus,
    ) -> PeerSyncStatus:
        return await self._sync_state.upsert(peer_id, status)

    async def list_sync_statuses(self) -> list[PeerSyncStatus]:
        return await self._sync_state.list_all()
