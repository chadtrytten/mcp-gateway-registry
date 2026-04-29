"""PostgreSQL repository for audit events.

Mirrors `registry/repositories/audit_repository.py::DocumentDBAuditRepository`
and satisfies the same `AuditRepositoryBase` ABC. Storage layout is the
single audit table defined in `migrations/postgres/postgres-B-tables-014-audit.sql`.

Two surfaces require non-trivial translation from the Mongo-shaped ABC:

1. **find()/count()/find_one()/distinct()** accept a Mongo filter dict that
   contains range operators (`$gte`/`$lte`) on the `timestamp` and
   `response.status_code` fields. The shared `mongo_filter.translate()` only
   covers the 8-op subset upstream uses for *registry* repos; audit needs
   range operators in addition. Rather than expand the shared translator
   (and bleed range ops into other repos), we keep a small audit-local
   builder that emits SQL for the bounded set of filter shapes
   `registry/audit/routes.py::_build_query` actually produces.

2. **aggregate(pipeline)** has no general Postgres equivalent. We dispatch by
   pipeline shape to one of seven hand-written SQL queries — every aggregation
   the upstream codebase emits today (all from `audit/routes.py:407-476`).
   Unknown shapes raise `NotImplementedError` per P3 §1.7.3 risk #5.
   Callsite inventory is in
   `ai-tasks/.../outputs/agent-BX-5-audit-pipeline-callsites.md`.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ..audit_repository import AuditRecord, AuditRepositoryBase
from .client import _table, get_pool

logger = logging.getLogger(__name__)


# Stream → log_type mapping for fields that should be projected from columns
# rather than JSONB. `identity_username` and `timestamp` are GENERATED ALWAYS
# / native columns; everything else lives under `data`.
_HOT_FIELDS: dict[str, str] = {
    "identity.username": "identity_username",
    "timestamp": "timestamp",
}


def _field_sql(path: str) -> str:
    """SQL fragment yielding the *text* value of a Mongo-style dotted path.

    Routes hot fields to dedicated columns; everything else uses JSONB
    extraction. Returned fragments are safe to interpolate — input paths
    are bounded by `_build_query`'s static field whitelist.
    """
    if path in _HOT_FIELDS:
        return _HOT_FIELDS[path]
    parts = path.split(".")
    if len(parts) == 1:
        return f"data->>'{parts[0]}'"
    return "data #>> '{" + ",".join(parts) + "}'"


def _field_int_sql(path: str) -> str:
    """SQL fragment yielding the *integer* value at a JSON path. Casts ::int."""
    return f"({_field_sql(path)})::int"


# ---------------------------------------------------------------------------
# Filter translation (find / find_one / count / distinct)
# ---------------------------------------------------------------------------


def _translate_audit_filter(
    filter_dict: dict[str, Any],
    *,
    start_param: int = 1,
) -> tuple[str, list[Any]]:
    """Audit-specific Mongo→SQL filter translator.

    Recognised shapes (anything else raises `NotImplementedError`):
      - {"log_type": str}
      - {"timestamp": {"$gte": dt, "$lte": dt}}                     (datetimes)
      - {"identity.username": str}
      - {"identity.username": {"$regex": str, "$options": "i"}}
      - {"action.operation": str}
      - {"action.resource_type": str}
      - {"action.resource_id": str}
      - {"authorization.decision": str}
      - {"mcp_request.method": str}
      - {"mcp_response.status": str}
      - {"mcp_server.name": {"$regex": str, "$options": "i"}}
      - {"response.status_code": {"$gte": int, "$lte": int}}
      - {"request_id": str}

    Returns ``(sql_fragment, params)``. SQL begins after `WHERE` (no leading
    keyword); `start_param` shifts ``$N`` numbering for use inside larger
    queries.
    """
    if not filter_dict:
        return "TRUE", []

    clauses: list[str] = []
    params: list[Any] = []

    def ph(value: Any) -> str:
        params.append(value)
        return f"${start_param + len(params) - 1}"

    for key, val in filter_dict.items():
        if key == "response.status_code" and isinstance(val, dict):
            field = _field_int_sql(key)
            for op, v in val.items():
                if op == "$gte":
                    clauses.append(f"{field} >= {ph(int(v))}")
                elif op == "$lte":
                    clauses.append(f"{field} <= {ph(int(v))}")
                else:
                    raise NotImplementedError(
                        f"Audit filter: unsupported op {op!r} on {key!r}"
                    )
        elif key == "timestamp" and isinstance(val, dict):
            for op, v in val.items():
                # asyncpg accepts datetime directly for TIMESTAMPTZ columns.
                if isinstance(v, str):
                    v = datetime.fromisoformat(v.replace("Z", "+00:00"))
                if op == "$gte":
                    clauses.append(f"timestamp >= {ph(v)}")
                elif op == "$lte":
                    clauses.append(f"timestamp <= {ph(v)}")
                else:
                    raise NotImplementedError(
                        f"Audit filter: unsupported op {op!r} on timestamp"
                    )
        elif isinstance(val, dict):
            # Operator-only dict: $regex (case-insensitive) is the only one
            # `_build_query` produces for non-range fields.
            if set(val.keys()) - {"$regex", "$options"}:
                raise NotImplementedError(
                    f"Audit filter: unsupported ops {list(val)!r} on {key!r}"
                )
            pattern = val.get("$regex")
            if not isinstance(pattern, str):
                raise NotImplementedError(
                    f"Audit filter: $regex on {key!r} requires a string pattern"
                )
            opts = val.get("$options", "")
            operator = "~*" if "i" in opts else "~"
            clauses.append(f"{_field_sql(key)} {operator} {ph(pattern)}")
        else:
            # Scalar equality — coerce to text since extraction yields text.
            if val is None:
                clauses.append(f"{_field_sql(key)} IS NULL")
            else:
                clauses.append(f"{_field_sql(key)} = {ph(str(val))}")

    if not clauses:
        return "TRUE", params
    if len(clauses) == 1:
        return clauses[0], params
    return "(" + " AND ".join(clauses) + ")", params


# ---------------------------------------------------------------------------
# Aggregation pipeline dispatcher
# ---------------------------------------------------------------------------


def _stage_op(stage: dict[str, Any]) -> str | None:
    """Return the single ``$``-prefixed key of a pipeline stage, or None."""
    if not isinstance(stage, dict) or len(stage) != 1:
        return None
    (key,) = stage.keys()
    return key if key.startswith("$") else None


def _shape(pipeline: list[dict[str, Any]]) -> tuple[str, ...]:
    """Tuple of stage operators — used as a switch key by ``aggregate()``."""
    return tuple(_stage_op(s) or "?" for s in pipeline)


def _group_field_path(group_id: Any) -> str | None:
    """Resolve ``$_id`` of a $group stage to a Mongo-style dotted path.

    ``"$identity.username"`` → ``"identity.username"``. ``$dateToString`` etc.
    return None — caller dispatches separately.
    """
    if isinstance(group_id, str) and group_id.startswith("$"):
        return group_id[1:]
    return None


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class PostgresAuditRepository(AuditRepositoryBase):
    """PostgreSQL implementation of :class:`AuditRepositoryBase`.

    Connection management follows the F-RegistryCardRepository pattern:
    lazy-resolve the singleton pool and acquire a fresh connection per call.
    """

    def __init__(self) -> None:
        self._table_name: str = _table("audit_events")
        logger.info(
            "Initialized Postgres AuditRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _row_to_doc(row: asyncpg.Record) -> dict[str, Any]:
        """Map a SELECT * row back to the Mongo-shaped audit document."""
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        # Hot columns are the source of truth for these fields — overlay them
        # so callers can't observe drift between the row and the JSONB body.
        ts = row["timestamp"]
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        data = dict(data)  # shallow copy; don't mutate asyncpg's view
        data["timestamp"] = ts
        data["_id"] = str(row["id"])
        return data

    # ------------------------------------------------------------------ ABC

    async def find(
        self,
        query: dict[str, Any],
        limit: int = 50,
        offset: int = 0,
        sort_field: str = "timestamp",
        sort_order: int = -1,
    ) -> list[dict[str, Any]]:
        logger.debug(
            "Postgres READ: Finding audit events with query=%s, limit=%d, offset=%d",
            query,
            limit,
            offset,
        )
        try:
            where, params = _translate_audit_filter(query, start_param=1)
        except NotImplementedError as exc:
            logger.error("Audit find: filter translation failed: %s", exc)
            return []

        # Sort field whitelist: the only sort upstream emits is `timestamp`.
        # Reject anything else so an unsanitised field can't reach the query.
        if sort_field != "timestamp":
            logger.warning(
                "Audit find: unsupported sort_field=%r; falling back to timestamp",
                sort_field,
            )
            sort_field = "timestamp"
        direction = "DESC" if sort_order == -1 else "ASC"

        sql = (
            f"SELECT id, data, timestamp FROM {self._table_name} "
            f"WHERE {where} "
            f"ORDER BY {sort_field} {direction} "
            f"LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        )
        params = [*params, int(limit), int(offset)]

        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error finding audit events: %s", exc, exc_info=True)
            return []
        except Exception as exc:  # noqa: BLE001 — match documentdb behavior
            logger.error("Error finding audit events: %s", exc, exc_info=True)
            return []

        docs = [self._row_to_doc(r) for r in rows]
        logger.debug("Postgres READ: Found %d audit events", len(docs))
        return docs

    async def find_one(
        self,
        query: dict[str, Any],
    ) -> dict[str, Any] | None:
        logger.debug("Postgres READ: Finding single audit event with query=%s", query)
        try:
            where, params = _translate_audit_filter(query, start_param=1)
        except NotImplementedError as exc:
            logger.error("Audit find_one: filter translation failed: %s", exc)
            return None

        sql = (
            f"SELECT id, data, timestamp FROM {self._table_name} "
            f"WHERE {where} ORDER BY timestamp DESC LIMIT 1"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                row = await conn.fetchrow(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error finding audit event: %s", exc, exc_info=True)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.error("Error finding audit event: %s", exc, exc_info=True)
            return None

        if row is None:
            logger.debug("Postgres READ: Audit event not found")
            return None
        return self._row_to_doc(row)

    async def count(
        self,
        query: dict[str, Any],
    ) -> int:
        logger.debug("Postgres READ: Counting audit events with query=%s", query)
        try:
            where, params = _translate_audit_filter(query, start_param=1)
        except NotImplementedError as exc:
            logger.error("Audit count: filter translation failed: %s", exc)
            return 0

        sql = f"SELECT COUNT(*) FROM {self._table_name} WHERE {where}"
        try:
            async with (await self._pool()).acquire() as conn:
                count = await conn.fetchval(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error counting audit events: %s", exc, exc_info=True)
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.error("Error counting audit events: %s", exc, exc_info=True)
            return 0

        logger.debug("Postgres READ: Counted %s audit events", count)
        return int(count or 0)

    async def distinct(
        self,
        field: str,
        query: dict[str, Any] | None = None,
    ) -> list[str]:
        logger.debug("Postgres READ: distinct on field=%s, query=%s", field, query)
        try:
            where, params = _translate_audit_filter(query or {}, start_param=1)
        except NotImplementedError as exc:
            logger.error("Audit distinct: filter translation failed: %s", exc)
            return []

        field_sql = _field_sql(field)
        # Skip empty/null values to mirror Mongo distinct() filter `if v`
        sql = (
            f"SELECT DISTINCT {field_sql} AS v FROM {self._table_name} "
            f"WHERE ({where}) AND {field_sql} IS NOT NULL "
            f"AND {field_sql} <> '' ORDER BY v ASC"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error in distinct(%s): %s", field, exc, exc_info=True)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.error("Error in distinct(%s): %s", field, exc, exc_info=True)
            return []

        result = [str(r["v"]) for r in rows]
        logger.debug("Postgres READ: Found %d distinct values for %s", len(result), field)
        return result

    async def aggregate(
        self,
        pipeline: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Translate a recognised Mongo pipeline shape to SQL.

        See module docstring for the supported set. Anything else raises
        ``NotImplementedError``.
        """
        logger.debug("Postgres READ: aggregate %d-stage pipeline", len(pipeline))
        try:
            return await self._dispatch_aggregate(pipeline)
        except NotImplementedError:
            raise
        except asyncpg.PostgresError as exc:
            logger.error("Postgres error in aggregate: %s", exc, exc_info=True)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.error("Error in aggregate: %s", exc, exc_info=True)
            return []

    async def _dispatch_aggregate(
        self,
        pipeline: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not pipeline or _stage_op(pipeline[0]) != "$match":
            raise NotImplementedError(
                "audit.aggregate: pipelines must start with $match (audit-only translator)"
            )

        match_stage = pipeline[0]["$match"]
        try:
            base_where, base_params = _translate_audit_filter(match_stage, start_param=1)
        except NotImplementedError as exc:
            raise NotImplementedError(
                f"audit.aggregate: $match has unsupported shape: {exc}"
            ) from exc

        shape = _shape(pipeline)

        # P1/P2/P7 — single-key $group + $sort + $limit.
        if shape == ("$match", "$group", "$sort", "$limit"):
            return await self._agg_group_sort_limit(
                pipeline, base_where, base_params,
            )

        # P3 — $group with $dateToString id, then $sort.
        if shape == ("$match", "$group", "$sort"):
            return await self._agg_group_date_sort(
                pipeline, base_where, base_params,
            )

        # P4 — bare $match + $group (mcp_access status distribution).
        if shape == ("$match", "$group"):
            return await self._agg_group_only(
                pipeline, base_where, base_params,
            )

        # P5 — registry_api status distribution: $project (with $switch) + $group.
        if shape == ("$match", "$project", "$group"):
            return await self._agg_project_switch_group(
                pipeline, base_where, base_params,
            )

        # P6 — per-user activity breakdown.
        if shape == (
            "$match", "$group", "$sort", "$group", "$sort", "$limit",
        ):
            return await self._agg_user_activity(
                pipeline, base_where, base_params,
            )

        raise NotImplementedError(
            f"audit.aggregate: unrecognised pipeline shape {shape!r}; "
            "see P3 §1.7.3 risk #5 — extend AuditRepositoryBase with a "
            "typed-params method instead of growing this translator."
        )

    # ------------------------------------------------------------------ shapes

    async def _agg_group_sort_limit(
        self,
        pipeline: list[dict[str, Any]],
        base_where: str,
        base_params: list[Any],
    ) -> list[dict[str, Any]]:
        """P1/P2/P7: $match → $group _id=<scalar> → $sort {count:-1} → $limit N."""
        group = pipeline[1]["$group"]
        path = _group_field_path(group.get("_id"))
        if path is None or group.get("count") != {"$sum": 1}:
            raise NotImplementedError(
                f"audit.aggregate: unsupported $group shape: {group!r}"
            )
        sort = pipeline[2]["$sort"]
        if sort != {"count": -1}:
            raise NotImplementedError(f"audit.aggregate: unsupported $sort: {sort!r}")
        limit = int(pipeline[3]["$limit"])

        field_sql = _field_sql(path)
        params = [*base_params, limit]
        limit_ph = f"${len(params)}"
        sql = (
            f"SELECT {field_sql} AS _id, COUNT(*) AS count "
            f"FROM {self._table_name} WHERE {base_where} "
            f"GROUP BY {field_sql} "
            f"ORDER BY count DESC LIMIT {limit_ph}"
        )
        return await self._fetch_records(sql, params)

    async def _agg_group_only(
        self,
        pipeline: list[dict[str, Any]],
        base_where: str,
        base_params: list[Any],
    ) -> list[dict[str, Any]]:
        """P4: $match → $group _id=<scalar>."""
        group = pipeline[1]["$group"]
        path = _group_field_path(group.get("_id"))
        if path is None or group.get("count") != {"$sum": 1}:
            raise NotImplementedError(
                f"audit.aggregate: unsupported $group shape: {group!r}"
            )
        field_sql = _field_sql(path)
        sql = (
            f"SELECT {field_sql} AS _id, COUNT(*) AS count "
            f"FROM {self._table_name} WHERE {base_where} "
            f"GROUP BY {field_sql}"
        )
        return await self._fetch_records(sql, base_params)

    async def _agg_group_date_sort(
        self,
        pipeline: list[dict[str, Any]],
        base_where: str,
        base_params: list[Any],
    ) -> list[dict[str, Any]]:
        """P3: $match → $group _id={$dateToString:{format:%Y-%m-%d, date:$timestamp}} → $sort {_id:1}."""
        group = pipeline[1]["$group"]
        gid = group.get("_id")
        ok = (
            isinstance(gid, dict)
            and list(gid.keys()) == ["$dateToString"]
            and gid["$dateToString"].get("format") == "%Y-%m-%d"
            and gid["$dateToString"].get("date") == "$timestamp"
            and group.get("count") == {"$sum": 1}
        )
        if not ok:
            raise NotImplementedError(
                f"audit.aggregate: unsupported date-bucket shape: {gid!r}"
            )
        sort = pipeline[2]["$sort"]
        if sort != {"_id": 1}:
            raise NotImplementedError(f"audit.aggregate: unsupported $sort: {sort!r}")

        sql = (
            f"SELECT to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS _id, "
            f"COUNT(*) AS count "
            f"FROM {self._table_name} WHERE {base_where} "
            f"GROUP BY 1 ORDER BY 1 ASC"
        )
        return await self._fetch_records(sql, base_params)

    async def _agg_project_switch_group(
        self,
        pipeline: list[dict[str, Any]],
        base_where: str,
        base_params: list[Any],
    ) -> list[dict[str, Any]]:
        """P5: registry_api status distribution.

        Validates that the $switch's branches match the documented 2xx/4xx/5xx
        bucketing on ``response.status_code``; reuses the inline CASE.
        """
        project = pipeline[1]["$project"]
        group = pipeline[2]["$group"]
        bucket = project.get("bucket")
        ok_project = (
            isinstance(bucket, dict)
            and "$switch" in bucket
            and isinstance(bucket["$switch"].get("branches"), list)
            and bucket["$switch"].get("default") == "other"
        )
        ok_group = (
            group.get("_id") == "$bucket"
            and group.get("count") == {"$sum": 1}
        )
        if not (ok_project and ok_group):
            raise NotImplementedError(
                "audit.aggregate: $project/$switch shape doesn't match the "
                "registry_api status-bucket recogniser"
            )
        # Hard-code the bucketing — the only $switch we actually translate is
        # the documented 2xx/4xx/5xx one. Anything else is rejected upstream
        # by the recogniser via the typed-params escape hatch.
        sql = (
            "WITH buckets AS ("
            f"  SELECT CASE "
            f"    WHEN (data #>> '{{response,status_code}}')::int BETWEEN 200 AND 299 THEN '2xx' "
            f"    WHEN (data #>> '{{response,status_code}}')::int BETWEEN 400 AND 499 THEN '4xx' "
            f"    WHEN (data #>> '{{response,status_code}}')::int >= 500 THEN '5xx' "
            f"    ELSE 'other' END AS bucket "
            f"  FROM {self._table_name} WHERE {base_where}"
            ") "
            "SELECT bucket AS _id, COUNT(*) AS count FROM buckets GROUP BY bucket"
        )
        return await self._fetch_records(sql, base_params)

    async def _agg_user_activity(
        self,
        pipeline: list[dict[str, Any]],
        base_where: str,
        base_params: list[Any],
    ) -> list[dict[str, Any]]:
        """P6: per-user activity breakdown.

        Validates the two-stage $group + $push shape, then runs the SQL CTE.
        ``op_field`` is read out of the *first* $group's _id.op so we honour
        the stream-specific op_field selection in audit/routes.py.
        """
        g1 = pipeline[1]["$group"]
        g2 = pipeline[3]["$group"]
        gid1 = g1.get("_id")
        ok = (
            isinstance(gid1, dict)
            and gid1.get("user") == "$identity.username"
            and isinstance(gid1.get("op"), str)
            and gid1["op"].startswith("$")
            and g1.get("count") == {"$sum": 1}
            and pipeline[2]["$sort"] == {"count": -1}
            and g2.get("_id") == "$_id.user"
            and g2.get("total") == {"$sum": "$count"}
            and isinstance(g2.get("operations"), dict)
            and "$push" in g2["operations"]
            and pipeline[4]["$sort"] == {"total": -1}
        )
        if not ok:
            raise NotImplementedError(
                f"audit.aggregate: user-activity shape mismatch: {pipeline!r}"
            )
        op_field_path = gid1["op"][1:]
        op_sql = _field_sql(op_field_path)
        limit = int(pipeline[5]["$limit"])

        params = [*base_params, limit]
        limit_ph = f"${len(params)}"
        sql = (
            "WITH per_user_op AS ("
            f"  SELECT identity_username AS \"user\", {op_sql} AS op, "
            f"  COUNT(*) AS count "
            f"  FROM {self._table_name} WHERE {base_where} "
            f"  GROUP BY identity_username, {op_sql}"
            ") "
            "SELECT \"user\" AS _id, "
            "       SUM(count)::bigint AS total, "
            "       jsonb_agg(jsonb_build_object('name', op, 'count', count) "
            "                 ORDER BY count DESC) AS operations "
            "FROM per_user_op "
            "GROUP BY \"user\" "
            f"ORDER BY total DESC LIMIT {limit_ph}"
        )
        return await self._fetch_records(sql, params)

    async def _fetch_records(
        self,
        sql: str,
        params: list[Any],
    ) -> list[dict[str, Any]]:
        async with (await self._pool()).acquire() as conn:
            rows = await conn.fetch(sql, *params)
        out: list[dict[str, Any]] = []
        for row in rows:
            doc = dict(row)
            # asyncpg returns JSONB columns as strings unless a codec is set.
            ops = doc.get("operations")
            if isinstance(ops, str):
                doc["operations"] = json.loads(ops)
            out.append(doc)
        return out

    # ------------------------------------------------------------------ insert

    async def insert(
        self,
        record: AuditRecord,
    ) -> bool:
        """Insert an audit record.

        Returns True on success and on duplicate-request_id (matching the
        DocumentDB behaviour where the auth-validation + endpoint-execution
        pair can race on the same request_id). Returns False on unexpected
        DB errors.

        NOTE: Postgres-side dedup requires a unique index on
        ``(data->>'request_id')``. Until the migration adds one, duplicate
        insertions succeed silently. Track via P3 §1.7 follow-up.
        """
        request_id = record.request_id
        logger.debug("Postgres WRITE: Inserting audit event with request_id=%s", request_id)

        doc = record.model_dump(mode="json")
        # Pull out timestamp for the dedicated TIMESTAMPTZ column.
        ts_raw = doc.get("timestamp")
        if isinstance(ts_raw, str):
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        elif isinstance(ts_raw, datetime):
            ts = ts_raw if ts_raw.tzinfo else ts_raw.replace(tzinfo=UTC)
        else:
            ts = datetime.now(UTC)

        # event_type column: prefer explicit `event_type`, fall back to
        # `log_type` (Pydantic discriminator). The audit DDL comment lists
        # 'auth.login' / 'server.create' style values, but in practice the
        # upstream Pydantic models only set log_type — we use that as the
        # column value to preserve filterability.
        event_type = doc.get("event_type") or doc.get("log_type") or "unknown"

        payload = json.dumps(doc, default=str)

        sql = (
            f"INSERT INTO {self._table_name} (event_type, timestamp, data) "
            "VALUES ($1, $2, $3::jsonb)"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(sql, event_type, ts, payload)
            logger.info(
                "Postgres WRITE: Inserted audit event request_id=%s", request_id
            )
            return True
        except asyncpg.UniqueViolationError:
            # Hits when (and only when) a unique index on data->>'request_id'
            # is in place. Silently succeed for parity with DocumentDB.
            logger.debug(
                "Postgres WRITE: Skipped duplicate audit event for request_id=%s",
                request_id,
            )
            return True
        except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as exc:
            logger.error(
                "Postgres connection error inserting audit event: %s", exc
            )
            return False
        except Exception as exc:  # noqa: BLE001 — match documentdb behaviour
            logger.error("Error inserting audit event: %s", exc, exc_info=True)
            return False
