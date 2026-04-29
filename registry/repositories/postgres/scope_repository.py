"""PostgreSQL repository for authorization scopes / Keycloak group mappings.

Mirrors ``registry/repositories/documentdb/scope_repository.py`` and satisfies
``ScopeRepositoryBase`` (interfaces.py:307-670, 16 abstract methods +
``list_groups`` default).

Storage layout
--------------
Single table ``mcp_scopes_{namespace}`` keyed by ``id`` = scope/group name.
Three JSONB array/object fields are broken out as first-class columns to
support GIN containment indexes and atomic mutation via ``jsonb_set`` + the
``mcp_jsonb_array_add_unique`` / ``mcp_jsonb_array_remove_value`` helpers
(prelude §2). The full Pydantic-dump ``data`` column is kept in sync on every
write so generic ``find_with_filter`` callsites have one canonical surface.

Atomic JSONB mutations write *both* the broken-out column AND the matching
``data->'field'`` slot in a single UPDATE so a snapshot read of either is
consistent (P3 §3.4).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ..interfaces import ScopeRepositoryBase
from .client import get_pool, table_name

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class PostgresScopeRepository(ScopeRepositoryBase):
    """PostgreSQL/JSONB implementation of the scope repository.

    The DocumentDB impl maintains an in-process ``_scopes_cache`` for
    fast reads after ``load_all()``. We keep an equivalent cache so callers
    that rely on snapshot-after-load semantics (e.g. authorisation middleware
    booting before the auth-server) continue to work.
    """

    def __init__(self) -> None:
        self._table_name: str = table_name("mcp_scopes")
        self._scopes_cache: dict[str, Any] = {}
        logger.info(
            "Initialized Postgres ScopeRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _decode_jsonb(value: Any) -> Any:
        """asyncpg may return JSONB as dict/list (codec registered) or str."""
        if isinstance(value, (dict, list)):
            return value
        if value is None:
            return None
        return json.loads(value)

    def _data_for(
        self,
        *,
        group_name: str,
        description: str,
        server_access: list,
        group_mappings: list,
        ui_permissions: dict,
        agent_access: list | None = None,
    ) -> str:
        """Build the canonical ``data`` JSONB payload."""
        doc: dict[str, Any] = {
            "_id": group_name,
            "scope_type": "group",
            "description": description,
            "server_access": server_access,
            "group_mappings": group_mappings,
            "ui_permissions": ui_permissions,
            "agent_access": agent_access or [],
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        return json.dumps(doc)

    # ------------------------------------------------------------------ ABC

    async def load_all(self) -> None:
        """Hydrate the in-process cache from Postgres.

        Mirrors the DocumentDB impl which builds three logical sections:
        ``UI-Scopes`` (group → ui_permissions), ``group_mappings``
        (keycloak_group → [scope_names]), and ``<scope_name>`` →
        ``access_rules`` flattened from ``server_access``.
        """
        sql = f"""
            SELECT id, ui_permissions, server_access, group_mappings
            FROM {self._table_name}
        """
        cache: dict[str, Any] = {"UI-Scopes": {}, "group_mappings": {}}
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error loading scopes: %s", exc)
            self._scopes_cache = cache
            return
        except Exception as exc:  # noqa: BLE001
            logger.error("Error loading scopes from Postgres: %s", exc, exc_info=True)
            self._scopes_cache = cache
            return

        for row in rows:
            scope_name = row["id"]
            ui_permissions = self._decode_jsonb(row["ui_permissions"]) or {}
            server_access = self._decode_jsonb(row["server_access"]) or []
            group_mappings = self._decode_jsonb(row["group_mappings"]) or []

            if ui_permissions:
                cache["UI-Scopes"][scope_name] = ui_permissions

            for keycloak_group in group_mappings:
                bucket = cache["group_mappings"].setdefault(keycloak_group, [])
                if scope_name not in bucket:
                    bucket.append(scope_name)

            if server_access:
                cache[scope_name] = server_access

        self._scopes_cache = cache
        logger.info("Loaded %d scopes from Postgres", len(rows))

    async def get_ui_scopes(self, group_name: str) -> dict[str, Any]:
        sql = f"SELECT ui_permissions FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, group_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error getting UI scopes: %s", exc)
            return {}
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting UI scopes for '%s': %s", group_name, exc, exc_info=True)
            return {}

        if row is None:
            return {}
        return self._decode_jsonb(row["ui_permissions"]) or {}

    async def get_group_mappings(self, keycloak_group: str) -> list[str]:
        # Containment match leverages the GIN index on group_mappings.
        sql = f"""
            SELECT id FROM {self._table_name}
            WHERE group_mappings @> to_jsonb($1::text)
            ORDER BY id
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, keycloak_group)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error getting group mappings: %s", exc)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error getting group mappings for '%s': %s",
                keycloak_group,
                exc,
                exc_info=True,
            )
            return []

        return [row["id"] for row in rows]

    async def get_server_scopes(self, scope_name: str) -> list[dict[str, Any]]:
        """Flatten server_access entries (handles new + legacy formats)."""
        sql = f"SELECT server_access FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, scope_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error getting server scopes: %s", exc)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error getting server scopes for '%s': %s",
                scope_name,
                exc,
                exc_info=True,
            )
            return []

        if row is None:
            return []

        server_access = self._decode_jsonb(row["server_access"]) or []
        flat: list[dict[str, Any]] = []
        for entry in server_access:
            if "access_rules" in entry:
                flat.extend(entry.get("access_rules", []))
            elif "server" in entry:
                flat.append(entry)
            # Else: skip non-server entries (e.g. agent permissions).
        return flat

    async def add_server_scope(
        self,
        server_path: str,
        scope_name: str,
        methods: list[str],
        tools: list[str] | None = None,
    ) -> bool:
        """Append a server-access entry to every scope row.

        Mirrors Mongo's ``update_many({}, {"$push": {"server_access": ...}})``.
        """
        server_name = server_path.lstrip("/")
        server_entry = {"server": server_name, "methods": methods, "tools": tools}
        scope_entry = {"scope_name": scope_name, "access_rules": [server_entry]}
        addition = json.dumps([scope_entry])

        sql = f"""
            UPDATE {self._table_name}
               SET server_access = COALESCE(server_access, '[]'::jsonb) || $1::jsonb,
                   data          = jsonb_set(
                                       data,
                                       '{{server_access}}',
                                       COALESCE(data->'server_access', '[]'::jsonb) || $1::jsonb
                                   )
        """
        try:
            async with (await self._pool()).acquire() as conn:
                await conn.execute(sql, addition)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error adding server scope: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to add server scope: %s", exc, exc_info=True)
            return False

        self._scopes_cache.setdefault(scope_name, []).append(server_entry)
        logger.info("Added server '%s' to scope '%s'", server_name, scope_name)
        return True

    async def remove_server_scope(self, server_path: str, scope_name: str) -> bool:
        """Remove the matching scope-entry row by name + server.

        Mongo's filter uses ``$pull`` with two predicates; we walk the array
        in SQL with ``jsonb_array_elements`` and re-aggregate.
        """
        server_name = server_path.lstrip("/")
        sql = f"""
            UPDATE {self._table_name} SET
                server_access = COALESCE(
                    (
                        SELECT jsonb_agg(elem)
                          FROM jsonb_array_elements(server_access) elem
                         WHERE NOT (
                             elem->>'scope_name' = $1
                             AND elem->'access_rules' @> jsonb_build_array(jsonb_build_object('server', $2::text))
                         )
                    ),
                    '[]'::jsonb
                ),
                data = jsonb_set(
                    data,
                    '{{server_access}}',
                    COALESCE(
                        (
                            SELECT jsonb_agg(elem)
                              FROM jsonb_array_elements(data->'server_access') elem
                             WHERE NOT (
                                 elem->>'scope_name' = $1
                                 AND elem->'access_rules' @> jsonb_build_array(jsonb_build_object('server', $2::text))
                             )
                        ),
                        '[]'::jsonb
                    )
                )
        """
        try:
            async with (await self._pool()).acquire() as conn:
                await conn.execute(sql, scope_name, server_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error removing server scope: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to remove server scope: %s", exc, exc_info=True)
            return False

        if scope_name in self._scopes_cache:
            self._scopes_cache[scope_name] = [
                s for s in self._scopes_cache[scope_name] if s.get("server") != server_name
            ]
        logger.info("Removed server '%s' from scope '%s'", server_name, scope_name)
        return True

    async def create_group(self, group_name: str, description: str = "") -> bool:
        sql = f"""
            INSERT INTO {self._table_name} (
                id, ui_permissions, server_access, group_mappings, description, data
            )
            VALUES ($1, '{{}}'::jsonb, '[]'::jsonb, '[]'::jsonb, $2, $3::jsonb)
        """
        data = self._data_for(
            group_name=group_name,
            description=description,
            server_access=[],
            group_mappings=[],
            ui_permissions={},
        )
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(sql, group_name, description, data)
        except asyncpg.UniqueViolationError:
            logger.error("Group already exists: %s", group_name)
            return False
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error creating group: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to create group '%s': %s", group_name, exc, exc_info=True)
            return False

        self._scopes_cache.setdefault("UI-Scopes", {})[group_name] = {}
        self._scopes_cache.setdefault("group_mappings", {})[group_name] = []
        logger.info("Created group '%s'", group_name)
        return True

    async def delete_group(
        self,
        group_name: str,
        remove_from_mappings: bool = True,
    ) -> bool:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, group_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error deleting group: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to delete group '%s': %s", group_name, exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Group '%s' not found", group_name)
            return False

        self._scopes_cache.get("UI-Scopes", {}).pop(group_name, None)
        self._scopes_cache.get("group_mappings", {}).pop(group_name, None)
        logger.info("Deleted group '%s'", group_name)
        return True

    async def get_group(self, group_name: str) -> dict[str, Any] | None:
        sql = f"""
            SELECT id, ui_permissions, server_access, group_mappings,
                   description, data, created_at, updated_at
            FROM {self._table_name} WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, group_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error getting group: %s", exc)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting group '%s': %s", group_name, exc, exc_info=True)
            return None

        if row is None:
            return None

        # Parity with Mongo: rename _id → scope_name in returned dict.
        doc = self._decode_jsonb(row["data"]) or {}
        doc.pop("_id", None)
        doc["scope_name"] = row["id"]
        # Authoritative columns override the JSONB body in case of skew.
        doc["ui_permissions"] = self._decode_jsonb(row["ui_permissions"]) or {}
        doc["server_access"] = self._decode_jsonb(row["server_access"]) or []
        doc["group_mappings"] = self._decode_jsonb(row["group_mappings"]) or []
        if row["description"] is not None:
            doc["description"] = row["description"]
        doc["created_at"] = row["created_at"]
        doc["updated_at"] = row["updated_at"]
        return doc

    async def list_groups(self) -> dict[str, Any]:
        sql = f"""
            SELECT id, ui_permissions, server_access, group_mappings
            FROM {self._table_name}
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error listing groups: %s", exc)
            return {}
        except Exception as exc:  # noqa: BLE001
            logger.error("Error listing groups: %s", exc, exc_info=True)
            return {}

        groups: dict[str, Any] = {}
        for row in rows:
            server_access = self._decode_jsonb(row["server_access"]) or []
            groups[row["id"]] = {
                "server_count": len(server_access),
                "ui_scopes": self._decode_jsonb(row["ui_permissions"]) or {},
                "mappings": self._decode_jsonb(row["group_mappings"]) or [],
            }
        return groups

    async def group_exists(self, group_name: str) -> bool:
        sql = f"SELECT 1 FROM {self._table_name} WHERE id = $1 LIMIT 1"
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, group_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error checking group: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Error checking group existence '%s': %s", group_name, exc, exc_info=True)
            return False

        return row is not None

    async def add_server_to_ui_scopes(self, group_name: str, server_name: str) -> bool:
        """Atomic add to ui_permissions.list_service array (idempotent)."""
        sql = f"""
            UPDATE {self._table_name}
               SET ui_permissions = jsonb_set(
                       COALESCE(ui_permissions, '{{}}'::jsonb),
                       '{{list_service}}',
                       mcp_jsonb_array_add_unique(
                           ui_permissions->'list_service',
                           to_jsonb($2::text)
                       )
                   ),
                   data = jsonb_set(
                       data,
                       '{{ui_permissions, list_service}}',
                       mcp_jsonb_array_add_unique(
                           data#>'{{ui_permissions, list_service}}',
                           to_jsonb($2::text)
                       )
                   )
             WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, group_name, server_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error adding server to UI scopes: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to add server to UI scopes: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Group '%s' not found", group_name)
            return False
        logger.info("Added server '%s' to UI scopes for group '%s'", server_name, group_name)
        return True

    async def remove_server_from_ui_scopes(self, group_name: str, server_name: str) -> bool:
        """Atomic remove from ui_permissions.list_service (idempotent)."""
        sql = f"""
            UPDATE {self._table_name}
               SET ui_permissions = jsonb_set(
                       COALESCE(ui_permissions, '{{}}'::jsonb),
                       '{{list_service}}',
                       mcp_jsonb_array_remove_value(
                           ui_permissions->'list_service',
                           to_jsonb($2::text)
                       )
                   ),
                   data = jsonb_set(
                       data,
                       '{{ui_permissions, list_service}}',
                       mcp_jsonb_array_remove_value(
                           data#>'{{ui_permissions, list_service}}',
                           to_jsonb($2::text)
                       )
                   )
             WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, group_name, server_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error removing server from UI scopes: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to remove server from UI scopes: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Group '%s' not found", group_name)
            return False
        logger.info("Removed server '%s' from UI scopes for group '%s'", server_name, group_name)
        return True

    async def add_group_mapping(self, group_name: str, scope_name: str) -> bool:
        """Atomic add to group_mappings array (idempotent)."""
        sql = f"""
            UPDATE {self._table_name}
               SET group_mappings = mcp_jsonb_array_add_unique(
                       group_mappings, to_jsonb($2::text)
                   ),
                   data = jsonb_set(
                       data,
                       '{{group_mappings}}',
                       mcp_jsonb_array_add_unique(
                           data->'group_mappings',
                           to_jsonb($2::text)
                       )
                   )
             WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, group_name, scope_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error adding group mapping: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to add group mapping: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Group '%s' not found", group_name)
            return False
        logger.info("Added mapping '%s' to group '%s'", scope_name, group_name)
        return True

    async def remove_group_mapping(self, group_name: str, scope_name: str) -> bool:
        """Atomic remove from group_mappings (idempotent)."""
        sql = f"""
            UPDATE {self._table_name}
               SET group_mappings = mcp_jsonb_array_remove_value(
                       group_mappings, to_jsonb($2::text)
                   ),
                   data = jsonb_set(
                       data,
                       '{{group_mappings}}',
                       mcp_jsonb_array_remove_value(
                           data->'group_mappings',
                           to_jsonb($2::text)
                       )
                   )
             WHERE id = $1
        """
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, group_name, scope_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error removing group mapping: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to remove group mapping: %s", exc, exc_info=True)
            return False

        if result.endswith(" 0"):
            logger.error("Group '%s' not found", group_name)
            return False
        logger.info("Removed mapping '%s' from group '%s'", scope_name, group_name)
        return True

    async def get_all_group_mappings(self) -> dict[str, list[str]]:
        sql = f"SELECT id, group_mappings FROM {self._table_name}"
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error getting all mappings: %s", exc)
            return {}
        except Exception as exc:  # noqa: BLE001
            logger.error("Error getting all group mappings: %s", exc, exc_info=True)
            return {}

        return {
            row["id"]: (self._decode_jsonb(row["group_mappings"]) or [])
            for row in rows
        }

    async def add_server_to_multiple_scopes(
        self,
        server_path: str,
        scope_names: list[str],
        methods: list[str],
        tools: list[str],
    ) -> bool:
        """Bulk-add a server to multiple scopes inside one transaction.

        Mongo loops sequential ``add_server_scope`` calls; we keep the
        loop but wrap it in a single transaction so partial failures roll
        back cleanly.
        """
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    for scope_name in scope_names:
                        ok = await self._add_server_scope_in_conn(
                            conn, server_path, scope_name, methods, tools
                        )
                        if not ok:
                            raise RuntimeError(
                                f"add_server_scope failed for '{scope_name}'"
                            )
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error in bulk add: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to add server to multiple scopes: %s", exc, exc_info=True)
            return False

        for scope_name in scope_names:
            self._scopes_cache.setdefault(scope_name, []).append(
                {"server": server_path.lstrip("/"), "methods": methods, "tools": tools}
            )
        return True

    async def _add_server_scope_in_conn(
        self,
        conn: asyncpg.Connection,
        server_path: str,
        scope_name: str,
        methods: list[str],
        tools: list[str] | None,
    ) -> bool:
        server_name = server_path.lstrip("/")
        scope_entry = {
            "scope_name": scope_name,
            "access_rules": [
                {"server": server_name, "methods": methods, "tools": tools},
            ],
        }
        addition = json.dumps([scope_entry])
        sql = f"""
            UPDATE {self._table_name}
               SET server_access = COALESCE(server_access, '[]'::jsonb) || $1::jsonb,
                   data          = jsonb_set(
                                       data,
                                       '{{server_access}}',
                                       COALESCE(data->'server_access', '[]'::jsonb) || $1::jsonb
                                   )
        """
        await conn.execute(sql, addition)
        return True

    async def remove_server_from_all_scopes(self, server_path: str) -> bool:
        """Remove every server_access entry referencing this server.

        Walks ``server_access`` in SQL: keeps entries whose ``access_rules``
        do NOT contain the server. Mirrors Mongo's
        ``$pull: {server_access: {access_rules.server: server_name}}``.
        """
        server_name = server_path.lstrip("/")
        sql = f"""
            UPDATE {self._table_name} SET
                server_access = COALESCE(
                    (
                        SELECT jsonb_agg(elem)
                          FROM jsonb_array_elements(server_access) elem
                         WHERE NOT (
                             elem->'access_rules' @> jsonb_build_array(jsonb_build_object('server', $1::text))
                         )
                    ),
                    '[]'::jsonb
                ),
                data = jsonb_set(
                    data,
                    '{{server_access}}',
                    COALESCE(
                        (
                            SELECT jsonb_agg(elem)
                              FROM jsonb_array_elements(data->'server_access') elem
                             WHERE NOT (
                                 elem->'access_rules' @> jsonb_build_array(jsonb_build_object('server', $1::text))
                             )
                        ),
                        '[]'::jsonb
                    )
                )
        """
        try:
            async with (await self._pool()).acquire() as conn:
                await conn.execute(sql, server_name)
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error("Postgres connection error removing server from all scopes: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to remove server from all scopes: %s", exc, exc_info=True)
            return False

        for scope_name in list(self._scopes_cache.keys()):
            if scope_name in {"UI-Scopes", "group_mappings"}:
                continue
            self._scopes_cache[scope_name] = [
                s for s in self._scopes_cache[scope_name] if s.get("server") != server_name
            ]
        logger.info("Removed server '%s' from all scopes", server_name)
        return True
