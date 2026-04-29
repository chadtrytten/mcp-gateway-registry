"""Unit tests for PostgresAuditRepository.

These tests exercise the SQL/filter translation logic without booting a real
Postgres instance: an `AsyncMock` connection stands in for asyncpg and the
fixtures inspect the rendered SQL + bind parameters.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from registry.repositories.postgres.audit_repository import (
    PostgresAuditRepository,
    _translate_audit_filter,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _mock_pool(conn: AsyncMock) -> AsyncMock:
    """Wrap a connection in an asyncpg-shaped pool mock."""
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire = MagicMock(return_value=cm)
    return pool


def _mock_conn() -> AsyncMock:
    conn = AsyncMock()
    txn = MagicMock()
    txn.__aenter__ = AsyncMock(return_value=None)
    txn.__aexit__ = AsyncMock(return_value=None)
    conn.transaction = MagicMock(return_value=txn)
    return conn


@pytest.fixture
def repo_factory():
    """Build a PostgresAuditRepository with patched pool/table."""

    def _factory(conn: AsyncMock):
        repo = PostgresAuditRepository.__new__(PostgresAuditRepository)
        repo._table_name = "audit_events_default"
        pool = _mock_pool(conn)
        repo._pool = AsyncMock(return_value=pool)
        return repo

    return _factory


# ---------------------------------------------------------------------------
# Filter translator
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuditFilterTranslator:
    def test_empty_filter(self):
        sql, params = _translate_audit_filter({})
        assert sql == "TRUE"
        assert params == []

    def test_log_type_equality(self):
        sql, params = _translate_audit_filter({"log_type": "registry_api_access"})
        assert "data->>'log_type' = $1" in sql
        assert params == ["registry_api_access"]

    def test_timestamp_range(self):
        cutoff = datetime(2026, 4, 22, tzinfo=UTC)
        end = datetime(2026, 4, 29, tzinfo=UTC)
        sql, params = _translate_audit_filter(
            {"timestamp": {"$gte": cutoff, "$lte": end}}
        )
        assert "timestamp >= $1" in sql
        assert "timestamp <= $2" in sql
        assert params == [cutoff, end]

    def test_status_code_range_uses_int_cast(self):
        sql, params = _translate_audit_filter(
            {"response.status_code": {"$gte": 400, "$lte": 599}}
        )
        assert "(data #>> '{response,status_code}')::int >= $1" in sql
        assert "(data #>> '{response,status_code}')::int <= $2" in sql
        assert params == [400, 599]

    def test_username_regex_case_insensitive(self):
        sql, params = _translate_audit_filter(
            {"identity.username": {"$regex": "alice", "$options": "i"}}
        )
        # Hot column, not data->>...
        assert "identity_username ~* $1" in sql
        assert params == ["alice"]

    def test_combined_match(self):
        cutoff = datetime(2026, 4, 22, tzinfo=UTC)
        sql, params = _translate_audit_filter(
            {
                "log_type": "mcp_server_access",
                "timestamp": {"$gte": cutoff},
                "identity.username": {"$regex": "^bob$", "$options": "i"},
            }
        )
        assert sql.startswith("(") and sql.endswith(")")
        assert "data->>'log_type' = $1" in sql
        assert "timestamp >= $2" in sql
        assert "identity_username ~* $3" in sql
        assert params == ["mcp_server_access", cutoff, "^bob$"]

    def test_unsupported_op_raises(self):
        with pytest.raises(NotImplementedError):
            _translate_audit_filter({"data.foo": {"$exists": True}})

    def test_param_offset(self):
        sql, params = _translate_audit_filter({"log_type": "x"}, start_param=5)
        assert "= $5" in sql
        assert params == ["x"]


# ---------------------------------------------------------------------------
# find / count / find_one / distinct
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuditRead:
    @pytest.mark.asyncio
    async def test_find_returns_docs_in_descending_order(self, repo_factory):
        conn = _mock_conn()
        ts = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        conn.fetch = AsyncMock(
            return_value=[
                {"id": 7, "data": {"request_id": "r-1", "log_type": "x"}, "timestamp": ts},
            ]
        )
        repo = repo_factory(conn)

        out = await repo.find({"log_type": "x"}, limit=10, offset=0)

        assert out == [
            {"_id": "7", "request_id": "r-1", "log_type": "x", "timestamp": ts}
        ]
        sql = conn.fetch.call_args[0][0]
        assert "ORDER BY timestamp DESC" in sql
        assert "LIMIT $2 OFFSET $3" in sql

    @pytest.mark.asyncio
    async def test_find_swallows_postgres_errors(self, repo_factory):
        import asyncpg

        conn = _mock_conn()
        conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("boom"))
        repo = repo_factory(conn)

        out = await repo.find({"log_type": "x"})
        assert out == []

    @pytest.mark.asyncio
    async def test_count_returns_int(self, repo_factory):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=42)
        repo = repo_factory(conn)
        assert await repo.count({"log_type": "x"}) == 42

    @pytest.mark.asyncio
    async def test_find_one_returns_first_doc(self, repo_factory):
        conn = _mock_conn()
        ts = datetime(2026, 4, 28, tzinfo=UTC)
        conn.fetchrow = AsyncMock(
            return_value={"id": 1, "data": {"request_id": "r"}, "timestamp": ts}
        )
        repo = repo_factory(conn)
        out = await repo.find_one({"request_id": "r"})
        assert out["request_id"] == "r"

    @pytest.mark.asyncio
    async def test_distinct_returns_sorted_strings(self, repo_factory):
        conn = _mock_conn()
        conn.fetch = AsyncMock(
            return_value=[{"v": "alice"}, {"v": "bob"}, {"v": "carol"}]
        )
        repo = repo_factory(conn)
        out = await repo.distinct("identity.username", {"log_type": "x"})
        assert out == ["alice", "bob", "carol"]
        # Ensures ORDER BY hits the SQL.
        sql = conn.fetch.call_args[0][0]
        assert "ORDER BY v ASC" in sql


# ---------------------------------------------------------------------------
# aggregate() dispatcher
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuditAggregate:
    BASE_MATCH = {
        "log_type": "registry_api_access",
        "timestamp": {"$gte": datetime(2026, 4, 22, tzinfo=UTC)},
    }

    @pytest.mark.asyncio
    async def test_p1_top_users(self, repo_factory):
        conn = _mock_conn()
        conn.fetch = AsyncMock(
            return_value=[{"_id": "alice", "count": 5}, {"_id": "bob", "count": 3}]
        )
        repo = repo_factory(conn)

        out = await repo.aggregate([
            {"$match": self.BASE_MATCH},
            {"$group": {"_id": "$identity.username", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ])

        assert out == [{"_id": "alice", "count": 5}, {"_id": "bob", "count": 3}]
        sql = conn.fetch.call_args[0][0]
        assert "GROUP BY identity_username" in sql
        assert "ORDER BY count DESC" in sql

    @pytest.mark.asyncio
    async def test_p3_timeline(self, repo_factory):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[{"_id": "2026-04-22", "count": 12}])
        repo = repo_factory(conn)

        out = await repo.aggregate([
            {"$match": self.BASE_MATCH},
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ])
        assert out == [{"_id": "2026-04-22", "count": 12}]
        sql = conn.fetch.call_args[0][0]
        assert "to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD')" in sql

    @pytest.mark.asyncio
    async def test_p5_status_buckets(self, repo_factory):
        conn = _mock_conn()
        conn.fetch = AsyncMock(
            return_value=[{"_id": "2xx", "count": 7}, {"_id": "5xx", "count": 1}]
        )
        repo = repo_factory(conn)

        out = await repo.aggregate([
            {"$match": self.BASE_MATCH},
            {"$project": {"bucket": {"$switch": {
                "branches": [
                    {"case": {"$and": [{"$gte": ["$response.status_code", 200]},
                                       {"$lt": ["$response.status_code", 300]}]}, "then": "2xx"},
                    {"case": {"$and": [{"$gte": ["$response.status_code", 400]},
                                       {"$lt": ["$response.status_code", 500]}]}, "then": "4xx"},
                    {"case": {"$gte": ["$response.status_code", 500]}, "then": "5xx"},
                ],
                "default": "other",
            }}}},
            {"$group": {"_id": "$bucket", "count": {"$sum": 1}}},
        ])
        assert {r["_id"] for r in out} == {"2xx", "5xx"}
        sql = conn.fetch.call_args[0][0]
        assert "BETWEEN 200 AND 299" in sql
        assert "BETWEEN 400 AND 499" in sql
        assert ">= 500" in sql

    @pytest.mark.asyncio
    async def test_p6_user_activity(self, repo_factory):
        import json as _json

        conn = _mock_conn()
        # Simulate asyncpg returning JSONB as a string for `operations`.
        conn.fetch = AsyncMock(
            return_value=[{
                "_id": "alice",
                "total": 5,
                "operations": _json.dumps([
                    {"name": "create", "count": 3},
                    {"name": "delete", "count": 2},
                ]),
            }]
        )
        repo = repo_factory(conn)

        out = await repo.aggregate([
            {"$match": self.BASE_MATCH},
            {"$group": {
                "_id": {"user": "$identity.username", "op": "$action.operation"},
                "count": {"$sum": 1},
            }},
            {"$sort": {"count": -1}},
            {"$group": {
                "_id": "$_id.user",
                "total": {"$sum": "$count"},
                "operations": {"$push": {"name": "$_id.op", "count": "$count"}},
            }},
            {"$sort": {"total": -1}},
            {"$limit": 10},
        ])
        assert len(out) == 1
        assert out[0]["_id"] == "alice"
        assert out[0]["operations"][0]["name"] == "create"
        sql = conn.fetch.call_args[0][0]
        assert "WITH per_user_op" in sql
        assert "jsonb_agg" in sql
        assert "data #>> '{action,operation}'" in sql

    @pytest.mark.asyncio
    async def test_unknown_pipeline_raises(self, repo_factory):
        conn = _mock_conn()
        repo = repo_factory(conn)
        with pytest.raises(NotImplementedError):
            await repo.aggregate([{"$lookup": {}}])


# ---------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------


class _FakeRecord:
    """Stand-in for RegistryApiAccessRecord with the fields insert() reads."""

    def __init__(self, *, request_id="abc", log_type="registry_api_access", ts=None):
        self.request_id = request_id
        self._dump = {
            "request_id": request_id,
            "log_type": log_type,
            "timestamp": (ts or datetime(2026, 4, 28, 12, tzinfo=UTC)).isoformat(),
        }

    def model_dump(self, mode="json"):
        return dict(self._dump)


@pytest.mark.unit
class TestAuditInsert:
    @pytest.mark.asyncio
    async def test_insert_success(self, repo_factory):
        conn = _mock_conn()
        conn.execute = AsyncMock()
        repo = repo_factory(conn)
        rec = _FakeRecord(request_id="r-100")
        assert await repo.insert(rec) is True
        sql = conn.execute.call_args[0][0]
        # Hot column event_type written; data column carries full payload.
        assert "INSERT INTO audit_events_default" in sql
        assert "$1, $2, $3::jsonb" in sql

    @pytest.mark.asyncio
    async def test_insert_duplicate_returns_true(self, repo_factory):
        import asyncpg

        conn = _mock_conn()
        conn.execute = AsyncMock(
            side_effect=asyncpg.UniqueViolationError("dup")
        )
        repo = repo_factory(conn)
        assert await repo.insert(_FakeRecord(request_id="dup")) is True

    @pytest.mark.asyncio
    async def test_insert_unexpected_error_returns_false(self, repo_factory):
        conn = _mock_conn()
        conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
        repo = repo_factory(conn)
        assert await repo.insert(_FakeRecord()) is False
