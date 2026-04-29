"""PostgreSQL hybrid search repository (pgvector + tsvector).

Mirrors `registry/repositories/documentdb/search_repository.py::DocumentDBSearchRepository`
and satisfies the same `SearchRepositoryBase` ABC at
`registry/repositories/interfaces.py:856-979`.

Storage layout
--------------
One row per indexed entity in ``mcp_embeddings_{N}_{namespace}`` where ``N``
is the embedding dimension (`settings.embeddings_model_dimensions`). The
table is created up front by the POSTGRES-B migration; this repository does
not run DDL.

Hybrid scoring
--------------
The hybrid SQL pulls the top-K candidates from the HNSW index, then re-ranks
them with a `ts_rank_cd` boost on the generated `text_tsv` column. Final
score formula matches the DocumentDB implementation verbatim:

    score = clamp((cos + 1) / 2 + ts_rank_cd * 0.1, 0, 1)

Per-query `hnsw.ef_search` is bumped via ``SET LOCAL`` (transaction-scoped)
to the value of `settings.vector_search_ef_search`.

Result formatting (grouped servers/tools/agents/skills/virtual_servers,
soft-cap distribution, tool extraction limits) reuses the helpers from the
DocumentDB module so output shape is byte-identical.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg

from ...core.config import embedding_config, settings
from ...schemas.agent_models import AgentCard
from ...utils.metadata import flatten_metadata_to_text
from ..documentdb.search_repository import (  # behavior parity helpers
    _distribute_results,
    _tokenize_query,
    _tool_extraction_limit,
)
from ..interfaces import SearchRepositoryBase
from .client import _table, get_pool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_table_suffix(dim: int) -> str:
    """Validate the dimension before interpolating into a table name."""
    if not isinstance(dim, int) or dim <= 0 or dim > 65536:
        raise ValueError(f"unsafe embedding dimension: {dim!r}")
    return str(dim)


def _build_status_sql(
    include_draft: bool,
    include_deprecated: bool,
    include_disabled: bool,
    *,
    start_param: int,
) -> tuple[str, list[Any]]:
    """SQL fragment + params for status / enabled lifecycle filters.

    Returns ``("", [])`` when no filtering is needed. Otherwise the fragment
    starts with ``AND`` and references ``status``/``is_enabled`` columns
    (the canonical schema from `postgres-B-tables-010-embeddings.sql`).
    """
    parts: list[str] = []
    params: list[Any] = []

    excluded: list[str] = []
    if not include_draft:
        excluded.append("draft")
    if not include_deprecated:
        excluded.append("deprecated")
    if excluded:
        params.append(excluded)
        ph = f"${start_param + len(params) - 1}"
        parts.append(f"NOT (status = ANY({ph}::text[]))")

    if not include_disabled:
        parts.append("is_enabled = TRUE")

    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params


def _normalize_score(vector_score: float, ts_rank: float) -> float:
    """Final-score formula. Matches search_repository.py:1850-1856."""
    normalized_vector = (vector_score + 1.0) / 2.0
    boost = ts_rank * 0.1
    return max(0.0, min(1.0, normalized_vector + boost))


def _parse_json_field(value: Any) -> Any:
    """asyncpg may hand back JSONB as text; normalize to dict/list."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class PostgresSearchRepository(SearchRepositoryBase):
    """PostgreSQL/pgvector implementation of :class:`SearchRepositoryBase`."""

    def __init__(self) -> None:
        self._dim = _safe_table_suffix(settings.embeddings_model_dimensions)
        self._table_name: str = _table(f"mcp_embeddings_{self._dim}")
        self._embedding_model: Any = None
        self._embedding_unavailable: bool = False
        logger.info(
            "Initialized Postgres SearchRepository with table: %s",
            self._table_name,
        )

    async def _pool(self) -> asyncpg.Pool:
        return await get_pool()

    async def _get_embedding_model(self):
        """Lazy-load embedding client. Mirrors the DocumentDB impl."""
        if self._embedding_model is None:
            from ...embeddings import create_embeddings_client

            self._embedding_model = create_embeddings_client(
                provider=settings.embeddings_provider,
                model_name=settings.embeddings_model_name,
                model_dir=settings.embeddings_model_dir,
                api_key=settings.embeddings_api_key,
                api_base=settings.embeddings_api_base,
                aws_region=settings.embeddings_aws_region,
                embedding_dimension=settings.embeddings_model_dimensions,
            )
        return self._embedding_model

    # ------------------------------------------------------------------ ABC

    async def initialize(self) -> None:
        """Verify the embeddings table exists.

        DDL ownership belongs to the migration runner (POSTGRES-B). Here we
        only sanity-check that the table is present so a misconfigured
        deployment fails loudly instead of returning empty searches forever.
        """
        logger.info(
            "Initializing Postgres hybrid search on table: %s", self._table_name
        )
        sql = "SELECT to_regclass($1) IS NOT NULL"
        try:
            async with (await self._pool()).acquire() as conn:
                exists = await conn.fetchval(sql, self._table_name)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to verify search table: %s", exc, exc_info=True)
            return

        if not exists:
            logger.error(
                "Embeddings table %s missing — run postgres-B-tables-010 migration",
                self._table_name,
            )

    async def index_server(
        self,
        path: str,
        server_info: dict[str, Any],
        is_enabled: bool = False,
    ) -> None:
        text_parts = [
            server_info.get("server_name", ""),
            server_info.get("description", ""),
        ]
        tags = server_info.get("tags", []) or []
        if tags:
            text_parts.append("Tags: " + ", ".join(tags))
        for tool in server_info.get("tool_list", []) or []:
            text_parts.append(tool.get("name", ""))
            text_parts.append(tool.get("description", ""))
        metadata = server_info.get("metadata", {}) or {}
        if isinstance(metadata, dict) and metadata:
            for k, v in metadata.items():
                text_parts.append(f"{k}: {v}")

        text_for_embedding = " ".join(filter(None, text_parts))
        metadata_text = flatten_metadata_to_text(metadata)
        embedding = await self._safe_encode(text_for_embedding, label=server_info.get("server_name") or path)

        tools = [
            {
                "name": t.get("name"),
                "description": t.get("description"),
                "inputSchema": t.get("inputSchema") or t.get("schema", {}),
            }
            for t in (server_info.get("tool_list", []) or [])
        ]

        await self._upsert_row(
            id_=path,
            entity_type="mcp_server",
            name=server_info.get("server_name", ""),
            description=server_info.get("description", ""),
            tags=tags,
            metadata_text=metadata_text,
            is_enabled=is_enabled,
            status=server_info.get("status", "active"),
            text_for_embedding=text_for_embedding,
            embedding=embedding,
            tools=tools,
            metadata=server_info,
            indexed_at=server_info.get("updated_at") or server_info.get("registered_at"),
        )

    async def index_agent(
        self,
        path: str,
        agent_card: AgentCard,
        is_enabled: bool = False,
    ) -> None:
        text_parts = [agent_card.name, agent_card.description or ""]
        tags = agent_card.tags or []
        if tags:
            text_parts.append("Tags: " + ", ".join(tags))
        if agent_card.capabilities:
            text_parts.append("Capabilities: " + ", ".join(agent_card.capabilities))
        if agent_card.skills:
            for skill in agent_card.skills:
                text_parts.append(skill.name)
                if skill.description:
                    text_parts.append(skill.description)

        text_for_embedding = " ".join(filter(None, text_parts))
        agent_metadata = getattr(agent_card, "metadata", None) or {}
        agent_metadata_text = flatten_metadata_to_text(agent_metadata)
        embedding = await self._safe_encode(text_for_embedding, label=agent_card.name)

        await self._upsert_row(
            id_=path,
            entity_type="a2a_agent",
            name=agent_card.name,
            description=agent_card.description or "",
            tags=tags,
            metadata_text=agent_metadata_text,
            is_enabled=is_enabled,
            status=getattr(agent_card, "status", "active"),
            text_for_embedding=text_for_embedding,
            embedding=embedding,
            tools=[],
            metadata=agent_card.model_dump(mode="json"),
            indexed_at=agent_card.updated_at or agent_card.registered_at,
        )

    async def index_skill(
        self,
        path: str,
        skill: Any,
        is_enabled: bool = False,
    ) -> None:
        text_parts = [skill.name, skill.description]
        if skill.tags:
            text_parts.append(f"Tags: {', '.join(skill.tags)}")
        if skill.compatibility:
            text_parts.append(f"Compatibility: {skill.compatibility}")
        if skill.target_agents:
            text_parts.append(f"For: {', '.join(skill.target_agents)}")
        if skill.metadata and skill.metadata.author:
            text_parts.append(f"Author: {skill.metadata.author}")
        if skill.metadata and skill.metadata.extra:
            extra_text = flatten_metadata_to_text(skill.metadata.extra)
            if extra_text:
                text_parts.append(extra_text)

        text_for_embedding = " ".join(filter(None, text_parts))
        embedding = await self._safe_encode(text_for_embedding, label=skill.name)

        skill_metadata_parts: list[str] = []
        if skill.metadata and skill.metadata.author:
            skill_metadata_parts.append(f"author {skill.metadata.author}")
        if skill.metadata and skill.metadata.version:
            skill_metadata_parts.append(f"version {skill.metadata.version}")
        if skill.metadata and skill.metadata.extra:
            extra_text = flatten_metadata_to_text(skill.metadata.extra)
            if extra_text:
                skill_metadata_parts.append(extra_text)
        if skill.registry_name:
            skill_metadata_parts.append(f"registry {skill.registry_name}")
        skill_metadata_text = " ".join(skill_metadata_parts)

        visibility_value = skill.visibility
        if hasattr(visibility_value, "value"):
            visibility_value = visibility_value.value

        await self._upsert_row(
            id_=path,
            entity_type="skill",
            name=skill.name,
            description=skill.description,
            tags=skill.tags or [],
            metadata_text=skill_metadata_text,
            is_enabled=is_enabled,
            status=getattr(skill, "status", "active"),
            text_for_embedding=text_for_embedding,
            embedding=embedding,
            tools=[],
            metadata={
                "skill_md_url": str(skill.skill_md_url),
                "skill_md_raw_url": str(skill.skill_md_raw_url) if skill.skill_md_raw_url else None,
                "author": skill.metadata.author if skill.metadata else None,
                "version": skill.metadata.version if skill.metadata else None,
                "compatibility": skill.compatibility,
                "target_agents": skill.target_agents or [],
                "registry_name": skill.registry_name,
                "visibility": visibility_value,
                "allowed_groups": skill.allowed_groups or [],
                "owner": skill.owner,
                "health_status": skill.health_status,
                "last_checked_time": skill.last_checked_time.isoformat()
                if skill.last_checked_time else None,
            },
            indexed_at=skill.updated_at or skill.created_at,
        )

    async def remove_entity(
        self,
        path: str,
    ) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE id = $1"
        try:
            async with (await self._pool()).acquire() as conn:
                result = await conn.execute(sql, path)
        except asyncpg.PostgresError as exc:
            logger.error("Failed to remove entity %s: %s", path, exc, exc_info=True)
            return
        # asyncpg returns 'DELETE N' command tag.
        deleted = result.split()[-1] if isinstance(result, str) else "0"
        if deleted != "0":
            logger.info("Removed entity '%s' from search index", path)
        else:
            logger.warning("Entity '%s' not found in search index", path)

    async def search(
        self,
        query: str,
        entity_types: list[str] | None = None,
        max_results: int = 10,
        include_draft: bool = False,
        include_deprecated: bool = False,
        include_disabled: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """Hybrid vector + lexical search.

        Falls through to lexical-only when the embedding model is unavailable.
        """
        try:
            query_embedding: list[float] | None = None
            if not self._embedding_unavailable:
                try:
                    model = await self._get_embedding_model()
                    query_embedding = model.encode([query])[0].tolist()
                except Exception as embed_error:  # noqa: BLE001
                    logger.warning(
                        "Embedding model unavailable, falling back to lexical-only search: %s",
                        embed_error,
                    )
                    self._embedding_unavailable = True

            if query_embedding is None:
                return await self._lexical_only_search(
                    query,
                    entity_types,
                    max_results,
                    include_draft=include_draft,
                    include_deprecated=include_deprecated,
                    include_disabled=include_disabled,
                )

            return await self._hybrid_search(
                query=query,
                query_embedding=query_embedding,
                entity_types=entity_types,
                max_results=max_results,
                include_draft=include_draft,
                include_deprecated=include_deprecated,
                include_disabled=include_disabled,
            )
        except Exception as exc:  # noqa: BLE001 — defensive: never 500 the API
            logger.error("Failed to perform hybrid search: %s", exc, exc_info=True)
            return self._empty_grouped()

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _empty_grouped() -> dict[str, list[dict[str, Any]]]:
        return {"servers": [], "tools": [], "agents": [], "skills": [], "virtual_servers": []}

    async def _safe_encode(
        self,
        text: str,
        *,
        label: str,
    ) -> list[float]:
        """Encode text with the embedding model; degrade to empty list on error.

        The HNSW index allows NULL — empty list maps to NULL in `_upsert_row`.
        """
        if self._embedding_unavailable:
            return []
        try:
            model = await self._get_embedding_model()
            return model.encode([text])[0].tolist()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Embedding model unavailable, indexing '%s' without embeddings: %s",
                label, exc,
            )
            self._embedding_unavailable = True
            return []

    async def _upsert_row(
        self,
        *,
        id_: str,
        entity_type: str,
        name: str,
        description: str,
        tags: list[str],
        metadata_text: str,
        is_enabled: bool,
        status: str,
        text_for_embedding: str,
        embedding: list[float],
        tools: list[dict[str, Any]],
        metadata: dict[str, Any],
        indexed_at: Any,
    ) -> None:
        """Single INSERT … ON CONFLICT used by every index_* method.

        Generated columns (`text_tsv`) are not in the column list — Postgres
        owns them. ``embedding`` is passed as a Python list; the pgvector
        codec registered in `_setup_connection` handles conversion.
        """
        emb_value = embedding if embedding else None
        # `indexed_at` is sometimes a string (Mongo) and sometimes a datetime;
        # column default `now()` covers None.
        sql = (
            f"INSERT INTO {self._table_name} "
            "  (id, entity_type, name, description, tags, metadata_text, "
            "   is_enabled, status, text_for_embedding, embedding, "
            "   embedding_metadata, tools, metadata, indexed_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
            "        $11::jsonb, $12::jsonb, $13::jsonb, COALESCE($14, now())) "
            "ON CONFLICT (id) DO UPDATE SET "
            "    entity_type = EXCLUDED.entity_type, "
            "    name = EXCLUDED.name, "
            "    description = EXCLUDED.description, "
            "    tags = EXCLUDED.tags, "
            "    metadata_text = EXCLUDED.metadata_text, "
            "    is_enabled = EXCLUDED.is_enabled, "
            "    status = EXCLUDED.status, "
            "    text_for_embedding = EXCLUDED.text_for_embedding, "
            "    embedding = EXCLUDED.embedding, "
            "    embedding_metadata = EXCLUDED.embedding_metadata, "
            "    tools = EXCLUDED.tools, "
            "    metadata = EXCLUDED.metadata, "
            "    indexed_at = EXCLUDED.indexed_at"
        )
        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        sql,
                        id_,
                        entity_type,
                        name,
                        description,
                        list(tags or []),
                        metadata_text or "",
                        bool(is_enabled),
                        str(status or "active"),
                        text_for_embedding or "",
                        emb_value,
                        json.dumps(embedding_config.get_embedding_metadata()),
                        json.dumps(tools or []),
                        json.dumps(metadata or {}, default=str),
                        indexed_at,
                    )
            logger.info("Indexed %s '%s' for search", entity_type, name)
        except asyncpg.PostgresError as exc:
            logger.error(
                "Failed to index %s '%s': %s", entity_type, name, exc, exc_info=True,
            )

    # ------------------------------------------------------------------ search paths

    async def _hybrid_search(
        self,
        *,
        query: str,
        query_embedding: list[float],
        entity_types: list[str] | None,
        max_results: int,
        include_draft: bool,
        include_deprecated: bool,
        include_disabled: bool,
    ) -> dict[str, list[dict[str, Any]]]:
        """Vector + lexical re-rank hybrid search via a single CTE query."""
        ef_search = settings.vector_search_ef_search
        candidate_limit = max(max_results * 3, 50)

        # Build dynamic filter pieces.
        params: list[Any] = []

        # $1 = embedding
        params.append(query_embedding)
        emb_ph = f"${len(params)}"

        type_filter = ""
        if entity_types:
            params.append(list(entity_types))
            type_filter = f" AND entity_type = ANY(${len(params)}::text[])"

        status_filter, status_params = _build_status_sql(
            include_draft, include_deprecated, include_disabled,
            start_param=len(params) + 1,
        )
        params.extend(status_params)

        # Numeric LIMITs share the param tail.
        params.append(candidate_limit)
        cand_ph = f"${len(params)}"
        params.append(query)
        query_ph = f"${len(params)}"
        params.append(max_results)
        final_ph = f"${len(params)}"

        sql = f"""
            WITH vec AS (
                SELECT id, entity_type, name, description, tags, metadata_text,
                       is_enabled, status, tools, metadata, indexed_at,
                       1 - (embedding <=> {emb_ph}::vector) AS vector_score
                FROM {self._table_name}
                WHERE embedding IS NOT NULL{type_filter}{status_filter}
                ORDER BY embedding <=> {emb_ph}::vector
                LIMIT {cand_ph}
            ),
            scored AS (
                SELECT v.*,
                       ts_rank_cd(f.text_tsv, plainto_tsquery('english', {query_ph})) AS lexical_rank,
                       GREATEST(0.0, LEAST(1.0,
                           (v.vector_score + 1.0) / 2.0
                         + ts_rank_cd(f.text_tsv, plainto_tsquery('english', {query_ph})) * 0.1
                       )) AS final_score
                FROM vec v
                JOIN {self._table_name} f USING (id)
            )
            SELECT * FROM scored ORDER BY final_score DESC LIMIT {final_ph}
        """

        try:
            async with (await self._pool()).acquire() as conn:
                async with conn.transaction():
                    # SET LOCAL — scoped to the surrounding transaction only.
                    await conn.execute(
                        "SELECT set_config('hnsw.ef_search', $1, true)",
                        str(ef_search),
                    )
                    rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Hybrid search SQL failed: %s", exc, exc_info=True)
            return await self._lexical_only_search(
                query,
                entity_types,
                max_results,
                include_draft=include_draft,
                include_deprecated=include_deprecated,
                include_disabled=include_disabled,
            )

        logger.info(
            "Hybrid search returned %d candidates (k=%d, efSearch=%d) for query=%r",
            len(rows), candidate_limit, ef_search, query,
        )

        scored: list[tuple[dict[str, Any], float]] = []
        query_tokens = _tokenize_query(query)
        for row in rows:
            doc = self._row_to_doc(row, query_tokens=query_tokens)
            # SQL already computed the same formula as `_normalize_score`; trust it.
            scored.append((doc, float(row["final_score"] or 0.0)))

        scored.sort(key=lambda x: x[1], reverse=True)
        selected = _distribute_results(scored, max_results)
        grouped = self._format_grouped(selected, max_results)

        for key in grouped:
            grouped[key].sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

        logger.info(
            "Hybrid search for %r returned %d servers, %d tools, %d agents, "
            "%d skills, %d virtual_servers (max_results=%d)",
            query,
            len(grouped["servers"]),
            len(grouped["tools"]),
            len(grouped["agents"]),
            len(grouped["skills"]),
            len(grouped["virtual_servers"]),
            max_results,
        )
        return grouped

    async def _lexical_only_search(
        self,
        query: str,
        entity_types: list[str] | None = None,
        max_results: int = 10,
        include_draft: bool = False,
        include_deprecated: bool = False,
        include_disabled: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """Pure ts_rank_cd path used when embeddings are unavailable."""
        query_tokens = _tokenize_query(query)
        if not query_tokens:
            logger.info("Lexical search: no valid tokens from query %r", query)
            return self._empty_grouped()

        params: list[Any] = []
        params.append(query)
        query_ph = f"${len(params)}"

        type_filter = ""
        if entity_types:
            params.append(list(entity_types))
            type_filter = f" AND entity_type = ANY(${len(params)}::text[])"

        status_filter, status_params = _build_status_sql(
            include_draft, include_deprecated, include_disabled,
            start_param=len(params) + 1,
        )
        params.extend(status_params)

        candidate_limit = max(max_results * 3, 50)
        params.append(candidate_limit)
        limit_ph = f"${len(params)}"

        sql = f"""
            SELECT id, entity_type, name, description, tags, metadata_text,
                   is_enabled, status, tools, metadata, indexed_at,
                   ts_rank_cd(text_tsv, plainto_tsquery('english', {query_ph})) AS lexical_rank
            FROM {self._table_name}
            WHERE text_tsv @@ plainto_tsquery('english', {query_ph}){type_filter}{status_filter}
            ORDER BY lexical_rank DESC
            LIMIT {limit_ph}
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("Lexical search SQL failed: %s", exc, exc_info=True)
            return self._empty_grouped()

        scored: list[tuple[dict[str, Any], float]] = []
        for row in rows:
            doc = self._row_to_doc(row, query_tokens=query_tokens)
            ts = float(row["lexical_rank"] or 0.0)
            # ts_rank_cd already ranks in roughly [0, 1]; clamp to be safe so
            # downstream formatting/sorting matches the hybrid-path band.
            score = max(0.0, min(1.0, ts))
            scored.append((doc, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        selected = _distribute_results(scored, max_results)
        return self._format_grouped(selected, max_results)

    # ------------------------------------------------------------------ formatting

    def _row_to_doc(
        self,
        row: asyncpg.Record,
        *,
        query_tokens: list[str],
    ) -> dict[str, Any]:
        """Reshape an asyncpg Record into the dict shape `_format_grouped` expects.

        Also derives `matching_tools` from `tools` so the per-row formatter
        can mimic DocumentDB's behaviour without hitting the DB twice.
        """
        tools = _parse_json_field(row.get("tools")) or []
        metadata = _parse_json_field(row.get("metadata")) or {}
        matching_tools: list[dict[str, Any]] = []
        if query_tokens and tools:
            lowered = [t.lower() for t in query_tokens]
            for tool in tools:
                if not isinstance(tool, dict):
                    continue
                tool_name = (tool.get("name") or "").lower()
                tool_desc = (tool.get("description") or "").lower()
                if any(tok in tool_name or tok in tool_desc for tok in lowered):
                    matching_tools.append(
                        {
                            "tool_name": tool.get("name", ""),
                            "description": tool.get("description", ""),
                            "relevance_score": 1.0,
                            "match_context": tool.get("description")
                            or f"Tool: {tool.get('name', '')}",
                        }
                    )
        return {
            "_id": row["id"],
            "path": row["id"],
            "entity_type": row["entity_type"],
            "name": row.get("name") or "",
            "description": row.get("description") or "",
            "tags": list(row.get("tags") or []),
            "metadata_text": row.get("metadata_text") or "",
            "is_enabled": bool(row.get("is_enabled")),
            "status": row.get("status") or "active",
            "tools": tools,
            "metadata": metadata,
            "matching_tools": matching_tools,
        }

    def _format_grouped(
        self,
        selected: list[tuple[dict[str, Any], float]],
        max_results: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Assemble the grouped response shape expected by ``/search``.

        Mirrors the per-entity formatting in
        `documentdb/search_repository.py::_format_lexical_results` /
        `search` so callers see identical fields regardless of backend.
        """
        grouped = self._empty_grouped()
        tool_count = 0
        tool_limit = _tool_extraction_limit(max_results)

        for doc, relevance_score in selected:
            entity_type = doc.get("entity_type")
            metadata = doc.get("metadata", {}) or {}
            matching_tools = doc.get("matching_tools", []) or []

            if entity_type == "mcp_server":
                entry = {
                    "entity_type": "mcp_server",
                    "path": doc.get("path"),
                    "server_name": doc.get("name"),
                    "description": doc.get("description"),
                    "tags": doc.get("tags", []),
                    "num_tools": metadata.get("num_tools", 0),
                    "is_enabled": doc.get("is_enabled", False),
                    "relevance_score": relevance_score,
                    "match_context": doc.get("description"),
                    "matching_tools": matching_tools,
                    "proxy_pass_url": metadata.get("proxy_pass_url"),
                    "mcp_endpoint": metadata.get("mcp_endpoint"),
                    "sse_endpoint": metadata.get("sse_endpoint"),
                    "supported_transports": metadata.get("supported_transports", []),
                }
                grouped["servers"].append(entry)

                tool_schema_map = {
                    t.get("name", ""): t.get("inputSchema", {})
                    for t in (doc.get("tools") or [])
                    if isinstance(t, dict)
                }
                for tool in matching_tools:
                    if tool_count >= tool_limit:
                        break
                    tool_name = tool.get("tool_name", "")
                    grouped["tools"].append(
                        {
                            "entity_type": "tool",
                            "server_path": doc.get("path", ""),
                            "server_name": doc.get("name", ""),
                            "tool_name": tool_name,
                            "description": tool.get("description", ""),
                            "inputSchema": tool_schema_map.get(tool_name, {}),
                            "relevance_score": tool.get("relevance_score", relevance_score),
                            "match_context": tool.get("match_context", ""),
                        }
                    )
                    tool_count += 1

            elif entity_type == "a2a_agent":
                grouped["agents"].append(
                    {
                        "entity_type": "a2a_agent",
                        "path": doc.get("path"),
                        "agent_name": doc.get("name"),
                        "description": doc.get("description"),
                        "tags": doc.get("tags", []),
                        "skills": metadata.get("skills", []),
                        "visibility": metadata.get("visibility", "public"),
                        "trust_level": metadata.get("trust_level"),
                        "is_enabled": doc.get("is_enabled", False),
                        "relevance_score": relevance_score,
                        "match_context": doc.get("description"),
                        "agent_card": metadata.get("agent_card", {}),
                    }
                )

            elif entity_type == "skill":
                grouped["skills"].append(
                    {
                        "entity_type": "skill",
                        "path": doc.get("path"),
                        "skill_name": doc.get("name"),
                        "description": doc.get("description"),
                        "tags": doc.get("tags", []),
                        "skill_md_url": metadata.get("skill_md_url"),
                        "version": metadata.get("version"),
                        "author": metadata.get("author"),
                        "visibility": metadata.get("visibility", "public"),
                        "owner": metadata.get("owner"),
                        "is_enabled": doc.get("is_enabled", False),
                        "status": doc.get("status", "active"),
                        "relevance_score": relevance_score,
                        "match_context": doc.get("description"),
                    }
                )

            elif entity_type == "virtual_server":
                grouped["virtual_servers"].append(
                    {
                        "entity_type": "virtual_server",
                        "path": doc.get("path"),
                        "server_name": doc.get("name"),
                        "description": doc.get("description"),
                        "tags": doc.get("tags", []),
                        "num_tools": metadata.get("num_tools", 0),
                        "backend_count": metadata.get("backend_count", 0),
                        "backend_paths": metadata.get("backend_paths", []),
                        "is_enabled": doc.get("is_enabled", False),
                        "relevance_score": relevance_score,
                        "match_context": doc.get("description"),
                        "matching_tools": matching_tools,
                    }
                )

        return grouped

    # ------------------------------------------------------------------ tag search

    async def search_by_tags(
        self,
        tags: list[str],
        entity_types: list[str] | None = None,
        max_results: int = 10,
        include_draft: bool = False,
        include_deprecated: bool = False,
        include_disabled: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """Direct tag-array search via the GIN @> operator.

        Matches entities whose ``tags`` array contains every requested tag
        (case-insensitive — we lowercase both sides).
        """
        if not tags:
            return self._empty_grouped()

        lowered = [t.lower() for t in tags]
        params: list[Any] = []

        params.append(lowered)
        tag_ph = f"${len(params)}"

        type_filter = ""
        if entity_types:
            params.append(list(entity_types))
            type_filter = f" AND entity_type = ANY(${len(params)}::text[])"

        status_filter, status_params = _build_status_sql(
            include_draft, include_deprecated, include_disabled,
            start_param=len(params) + 1,
        )
        params.extend(status_params)

        params.append(max_results * 5)
        limit_ph = f"${len(params)}"

        # Lower-cased intersection: ARRAY(SELECT lower(t) FROM unnest(tags) t) @> lowered
        sql = f"""
            SELECT id, entity_type, name, description, tags, metadata_text,
                   is_enabled, status, tools, metadata, indexed_at
            FROM {self._table_name}
            WHERE ARRAY(SELECT lower(t) FROM unnest(tags) t) @> {tag_ph}::text[]
              {type_filter}{status_filter}
            LIMIT {limit_ph}
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.PostgresError as exc:
            logger.error("search_by_tags SQL failed: %s", exc, exc_info=True)
            return self._empty_grouped()

        scored = [(self._row_to_doc(r, query_tokens=lowered), 1.0) for r in rows]
        return self._format_grouped(scored[:max_results], max_results)

    async def get_all_tags(self) -> list[str]:
        sql = f"""
            SELECT DISTINCT lower(t) AS tag
            FROM {self._table_name}, unnest(tags) AS t
            WHERE t IS NOT NULL AND t <> ''
            ORDER BY tag ASC
        """
        try:
            async with (await self._pool()).acquire() as conn:
                rows = await conn.fetch(sql)
        except asyncpg.PostgresError as exc:
            logger.error("get_all_tags SQL failed: %s", exc, exc_info=True)
            return []
        return [r["tag"] for r in rows]


__all__ = [
    "PostgresSearchRepository",
    "_build_status_sql",
    "_normalize_score",
]
