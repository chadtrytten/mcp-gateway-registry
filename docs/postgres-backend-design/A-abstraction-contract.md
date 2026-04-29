# POSTGRES-A — Upstream Abstraction-Layer Contract Memo

**Source:** `agentic-community/mcp-gateway-registry` @ `d7cdc3b3` (main HEAD, 2026-04-28).
**Cloned to:** `/tmp/mcp-gateway-registry`. All paths in this memo are relative to that root.
**Author:** Opus subagent POSTGRES-A. **Heartbeat:** `agent-POSTGRES-A.tsv`.
**Mission:** Catalogue every contract a `PostgresRepository` family must satisfy to slot in beside the existing `file/` and `documentdb/` backends.

---

## §1 — Repository directory map

```
registry/repositories/                           29 files
├── __init__.py                                    0
├── interfaces.py                              1,479    ← 12 ABCs (most repos)
├── factory.py                                   390    ← STORAGE_BACKEND switch
├── audit_repository.py                          338    ← AuditRepositoryBase (ABC) + DocumentDBAuditRepository (impl)
├── stats_repository.py                          260    ← module-level fns (no ABC; branches on settings.storage_backend internally)
├── app_log_repository.py                        109    ← AppLogRepository (concrete, DocumentDB-only; no ABC)
├── file/                                       (7 files)
│   ├── __init__.py                                0
│   ├── server_repository.py                     410
│   ├── agent_repository.py                      219
│   ├── scope_repository.py                      569
│   ├── search_repository.py                     136    ← FaissSearchRepository (FAISS + sentence-transformers)
│   ├── security_scan_repository.py              109
│   ├── skill_security_scan_repository.py        109
│   ├── federation_config_repository.py          177
│   └── peer_federation_repository.py            426
└── documentdb/                                 (12 files)
    ├── __init__.py                               25
    ├── client.py                                 55    ← AsyncIOMotorClient singleton + namespacing
    ├── server_repository.py                     405
    ├── agent_repository.py                      318
    ├── scope_repository.py                      581
    ├── search_repository.py                   2,073    ← largest file; vector + lexical + hybrid + soft-cap distribution
    ├── security_scan_repository.py              156
    ├── skill_security_scan_repository.py        129
    ├── federation_config_repository.py          127
    ├── peer_federation_repository.py            354
    ├── skill_repository.py                      362    ← (no `file/` peer; factory falls back to DocumentDB)
    ├── virtual_server_repository.py             279    ← (no `file/` peer; factory falls back to DocumentDB)
    ├── backend_session_repository.py            236    ← TTL collection
    └── registry_card_repository.py               99    ← DocumentDB-only (factory does not branch)
```

Total Python LoC under `registry/repositories/`: ~9,116. Skipped: `__pycache__`, generated.

Two utility modules outside the package are load-bearing for the DocumentDB path:
- `registry/utils/mongodb_connection.py` (72 LoC) — connection-string + TLS builders shared by the async Motor client and the sync `MongoDBLogHandler`.
- `registry/utils/mongodb_log_handler.py` — sync log handler that writes `application_logs_{namespace}` with TTL on `created_at`. (Out of scope for the repository contract but is the *write* side of the read-only `AppLogRepository`.)

---

## §2 — Abstract base classes (verbatim signatures)

13 abstract base classes are declared (12 in `interfaces.py`, 1 in `audit_repository.py`). One additional repository (`AppLogRepository`) is concrete-only and DocumentDB-only — Postgres parity for it is optional. `stats_repository.py` exposes module-level functions, not an ABC; it branches internally on `settings.storage_backend` and would need a third branch added.

For each ABC below: full method list with verbatim Python signature and a one-line semantic.

### 2.1 `ServerRepositoryBase` — `interfaces.py:26-180`
```python
async def get(self, path: str) -> dict[str, Any] | None
async def list_all(self) -> dict[str, dict[str, Any]]
async def list_paginated(self, skip: int = 0, limit: int = 100) -> dict[str, dict[str, Any]]
async def list_by_source(self, source: str) -> dict[str, dict[str, Any]]
async def create(self, server_info: dict[str, Any]) -> bool
async def update(self, path: str, server_info: dict[str, Any]) -> bool
async def delete(self, path: str) -> bool
async def delete_with_versions(self, path: str) -> int     # deletes _id == path AND _id =~ ^path:
async def get_state(self, path: str) -> bool
async def set_state(self, path: str, enabled: bool) -> bool
async def load_all(self) -> None                            # warm cache / no-op for DB
async def count(self) -> int
async def update_field(self, path: str, field: str, value: Any) -> bool   # dot-notation, None unsets
async def find_with_filter(self, filter_dict: dict[str, Any]) -> dict[str, dict]   # raw Mongo-style filter
```
Semantics: keyed by `path` (used as Mongo `_id`). `delete_with_versions` matches both the active doc and any sibling docs whose `_id` matches `^{path}:` (regex anchored), see `documentdb/server_repository.py:285-294`.

### 2.2 `AgentRepositoryBase` — `interfaces.py:183-304`
```python
async def get(self, path: str) -> AgentCard | None
async def list_all(self) -> list[AgentCard]
async def list_paginated(self, skip: int = 0, limit: int = 100) -> list[AgentCard]
async def create(self, agent: AgentCard) -> AgentCard
async def update(self, path: str, updates: dict[str, Any]) -> AgentCard
async def delete(self, path: str) -> bool
async def get_state(self, path: str) -> bool
async def set_state(self, path: str, enabled: bool) -> bool
async def load_all(self) -> None
async def count(self) -> int
async def update_field(self, path: str, field: str, value: Any) -> bool
async def find_with_filter(self, filter_dict: dict[str, Any]) -> dict[str, dict]
```
Semantics: returns Pydantic v2 `AgentCard` instances (parsed from `_id` + doc body). The DocumentDB impl pops `_id` and reassigns to `path` before instantiating the Pydantic model — see `documentdb/agent_repository.py:53-55`.

### 2.3 `ScopeRepositoryBase` — `interfaces.py:307-669`
14 abstract methods + 1 concrete:
```python
async def get_ui_scopes(self, group_name: str) -> dict[str, Any]
async def get_group_mappings(self, keycloak_group: str) -> list[str]
async def get_server_scopes(self, scope_name: str) -> list[dict[str, Any]]
async def load_all(self) -> None
async def add_server_scope(self, server_path, scope_name, methods, tools=None) -> bool
async def remove_server_scope(self, server_path, scope_name) -> bool
async def create_group(self, group_name: str, description: str = "") -> bool
async def delete_group(self, group_name: str, remove_from_mappings: bool = True) -> bool
async def get_group(self, group_name: str) -> dict[str, Any]
async def list_groups(self) -> dict[str, Any]                  # NB: not @abstractmethod (concrete pass)
async def group_exists(self, group_name: str) -> bool
async def add_server_to_ui_scopes(self, group_name, server_name) -> bool
async def remove_server_from_ui_scopes(self, group_name, server_name) -> bool
async def add_group_mapping(self, group_name, scope_name) -> bool
async def remove_group_mapping(self, group_name, scope_name) -> bool
async def get_all_group_mappings(self) -> dict[str, list[str]]
async def add_server_to_multiple_scopes(self, server_path, scope_names, methods, tools) -> bool
async def remove_server_from_all_scopes(self, server_path: str) -> bool
```
Semantics: replaces the YAML scope file (`auth_server/scopes.yml`) with a `mcp-scopes` collection — sectioned data ("UI-Scopes", "group_mappings", "server_scopes"). Callers expect the original YAML structure to be reconstituted.

### 2.4 `SecurityScanRepositoryBase` — `interfaces.py:672-761`
```python
async def get(self, server_path: str) -> dict[str, Any] | None
async def list_all(self) -> list[dict[str, Any]]
async def create(self, scan_result: dict[str, Any]) -> bool        # upsert keyed on server_path
async def get_latest(self, server_path: str) -> dict[str, Any] | None
async def query_by_status(self, status: str) -> list[dict[str, Any]]
async def load_all(self) -> None
```

### 2.5 `SkillSecurityScanRepositoryBase` — `interfaces.py:764-853`
Identical shape to `SecurityScanRepositoryBase`, keyed on `skill_path` instead of `server_path`. (Discrete ABC because of separate collections / lifecycle.)

### 2.6 `SearchRepositoryBase` — `interfaces.py:856-978`
```python
async def initialize(self) -> None                                  # creates indexes, incl. HNSW vector idx
async def index_server(self, path, server_info, is_enabled=False) -> None
async def index_agent(self, path, agent_card: AgentCard, is_enabled=False) -> None
async def remove_entity(self, path: str) -> None
async def search(self, query, entity_types=None, max_results=10,
                  include_draft=False, include_deprecated=False,
                  include_disabled=False) -> dict[str, list[dict[str, Any]]]
# Concrete defaults (overridable):
async def index_skill(self, path, skill, is_enabled=False) -> None  # default no-op
async def index_virtual_server(self, path, virtual_server, is_enabled=False) -> None  # default no-op
async def search_by_tags(self, tags, entity_types=None, max_results=10, ...) -> dict
async def get_all_tags(self) -> list[str]                           # default returns []
```

### 2.7 `PeerFederationRepositoryBase` — `interfaces.py:981-1050`
```python
async def load_all(self) -> None
async def get_peer(self, peer_id: str) -> Any | None
async def list_peers(self, enabled: bool | None = None) -> list[Any]
async def create_peer(self, config: Any) -> Any
async def update_peer(self, peer_id: str, updates: dict[str, Any]) -> Any
async def delete_peer(self, peer_id: str) -> bool
async def get_sync_status(self, peer_id: str) -> Any | None
async def update_sync_status(self, peer_id: str, status: Any) -> Any
async def list_sync_statuses(self) -> list[Any]
```

### 2.8 `FederationConfigRepositoryBase` — `interfaces.py:1053-1106`
```python
async def get_config(self, config_id: str = "default") -> FederationConfig | None
async def save_config(self, config: FederationConfig, config_id: str = "default") -> FederationConfig
async def delete_config(self, config_id: str = "default") -> bool
async def list_configs(self) -> list[dict[str, Any]]
```

### 2.9 `SkillRepositoryBase` — `interfaces.py:1109-1236`
```python
async def ensure_indexes(self) -> None
async def get(self, path: str) -> SkillCard | None
async def list_all(self, skip: int = 0, limit: int = 100) -> list[SkillCard]
async def list_paginated(self, skip: int = 0, limit: int = 100) -> list[SkillCard]
async def list_filtered(self, include_disabled=False, tag=None,
                          visibility=None, registry_name=None) -> list[SkillCard]
async def create(self, skill: SkillCard) -> SkillCard
async def update(self, path: str, updates: dict[str, Any]) -> SkillCard | None
async def delete(self, path: str) -> bool
async def get_state(self, path: str) -> bool
async def set_state(self, path: str, enabled: bool) -> bool
async def create_many(self, skills: list[SkillCard]) -> list[SkillCard]   # batch insert
async def update_many(self, updates: dict[str, dict[str, Any]]) -> int     # batch upsert by path
async def count(self) -> int
```
Note: `factory.get_skill_repository` falls through to DocumentDB even when `storage_backend == "file"` (`factory.py:240-244`), so the `file/` directory has no skill peer. Postgres must implement this fully.

### 2.10 `BackendSessionRepositoryBase` — `interfaces.py:1239-1333`
```python
async def ensure_indexes(self) -> None                       # MUST create TTL on last_used_at
async def get_backend_session(self, client_session_id, backend_key) -> str | None
async def store_backend_session(self, client_session_id, backend_key,
                                  backend_session_id, user_id, virtual_server_path) -> None
async def delete_backend_session(self, client_session_id, backend_key) -> None
async def create_client_session(self, client_session_id, user_id, virtual_server_path) -> None
async def validate_client_session(self, client_session_id) -> bool
```
Postgres equivalent of TTL: scheduled job or partial index + `expires_at` column with periodic `DELETE WHERE expires_at < now()`.

### 2.11 `VirtualServerRepositoryBase` — `interfaces.py:1336-1457`
```python
async def ensure_indexes(self) -> None
async def get(self, path: str) -> VirtualServerConfig | None
async def list_all(self) -> list[VirtualServerConfig]
async def list_enabled(self) -> list[VirtualServerConfig]
async def create(self, config: VirtualServerConfig) -> VirtualServerConfig
async def update(self, path: str, updates: dict[str, Any]) -> VirtualServerConfig | None
async def delete(self, path: str) -> bool
async def get_state(self, path: str) -> bool
async def set_state(self, path: str, enabled: bool) -> bool
```
Same fallback caveat as `SkillRepositoryBase` (`factory.py:282-289`).

### 2.12 `RegistryCardRepositoryBase` — `interfaces.py:1460-1479`
```python
async def get(self) -> RegistryCard | None
async def save(self, card: RegistryCard) -> RegistryCard
async def exists(self) -> bool
```
Singleton document. `factory.get_registry_card_repository` is hard-wired to DocumentDB (`factory.py:330-335`) — Postgres must add a branch here, or this remains DocumentDB-only.

### 2.13 `AuditRepositoryBase` — `audit_repository.py:26-138`
```python
async def find(self, query: dict, limit=50, offset=0,
                sort_field="timestamp", sort_order=-1) -> list[dict]
async def find_one(self, query: dict) -> dict | None
async def count(self, query: dict) -> int
async def distinct(self, field: str, query: dict | None = None) -> list[str]
async def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]
async def insert(self, record: AuditRecord) -> bool
```
This ABC is the leakiest: `find`/`count` accept raw MongoDB query dicts, and `aggregate` accepts a Mongo aggregation pipeline. Callers in `registry/audit/routes.py` build pipelines like `{"$match": …}, {"$group": {"_id": "$identity.username", …}}` and pass them through (lines 405-475). See §5.

### 2.14 `AppLogRepository` (concrete, no ABC) — `app_log_repository.py:14-109`
Methods: `query(...)`, `get_distinct_services()`, `get_distinct_hostnames()`. Postgres parity is optional but called via `factory.get_app_log_repository()`; the factory currently returns `None` for non-Mongo backends (`factory.py:347-355`).

### 2.15 `stats_repository` (module-level, no ABC) — `stats_repository.py`
Public async fns: `increment_search_counter()`, `get_search_count()`, `get_search_counts()`. Internally branches on `settings.storage_backend in ("mongodb-ce", "documentdb")` vs file (lines 30, 45, 61). A third branch `== "postgres"` plus `_increment_postgres()` / `_get_count_postgres()` / `_get_counts_postgres()` is required.

---

## §3 — Factory wiring

`registry/repositories/factory.py` defines 14 module-level singletons (`_server_repo`, `_agent_repo`, …) and 14 `get_*_repository()` accessor functions plus `reset_repositories()` (used in tests, lines 360-390).

The selection idiom appears 12 times (lines 54-62 set the pattern), e.g. for `get_server_repository`:
```python
backend = settings.storage_backend
if backend in ("documentdb", "mongodb-ce"):
    from .documentdb.server_repository import DocumentDBServerRepository
    _server_repo = DocumentDBServerRepository()
else:
    from .file.server_repository import FileServerRepository
    _server_repo = FileServerRepository()
```

**`STORAGE_BACKEND` env-var → backend mapping** (verified against `core/config.py:477` default `"file"` and against the factory branches):

| `STORAGE_BACKEND` value | Branch taken | Connection string | Auth mechanism |
|---|---|---|---|
| `file` (default) | `else` → `file/*Repository` | n/a | n/a |
| `mongodb-ce` | `if backend in (…)` → `documentdb/*Repository` | `mongodb://…` (from `mongodb_connection.py:36-44`) | SCRAM-SHA-256 |
| `documentdb` | same | `mongodb://…` | SCRAM-SHA-1 (or MONGODB-AWS via `documentdb_use_iam`) |

The mongoDB-CE vs DocumentDB *behavioural* split is decided downstream (search repository falls back to client-side cosine on Mongo-CE; see §7).

**To add a fourth value `postgres`**, the minimal contract is:
1. Replace `if backend in ("documentdb", "mongodb-ce")` in 12 factory functions with a three-way `match`/`elif` selecting `from .postgres.<name>_repository import PostgresXxxRepository` when `backend == "postgres"`.
2. Add Postgres branches to `get_audit_repository` (currently returns `None` for non-Mongo at line 219), `get_backend_session_repository` (returns `None` at 316), `get_app_log_repository` (returns `None` at 355), and `get_registry_card_repository` (currently DocumentDB-hardcoded at 330).
3. Patch the two file-fallback fall-throughs in `get_skill_repository` (line 240) and `get_virtual_server_repository` (line 282) to *also* check for `postgres`.
4. Patch `stats_repository.py` lines 30, 45, 61 to include `"postgres"` and add `_increment_postgres / _get_count_postgres / _get_counts_postgres` that target a Postgres counters table.
5. Add `from .postgres.client import close_postgres_pool` and call it during shutdown alongside `close_documentdb_client` (closure currently lives in `registry/main.py` — out of repo scope but needs a parallel hook).

**Risk:** the string `("documentdb", "mongodb-ce")` literal is repeated 14 times in `factory.py` and 3 times in `stats_repository.py`. A helper `_is_mongo_backend(backend) -> bool` and `_is_postgres_backend` would be the natural refactor; the upstream codebase has not done this. Phase Β should consider extracting `_BACKEND_DISPATCH = {"file": …, "documentdb": …, "postgres": …}` once Postgres is added, since adding a fifth backend would otherwise duplicate the pattern again.

---

## §4 — Settings additions for Postgres

**Existing DocumentDB settings** (`core/config.py:477-493`):
```python
storage_backend: str = "file"            # "file" | "mongodb-ce" | "documentdb"
documentdb_host: str = "localhost"
documentdb_port: int = 27017
documentdb_database: str = "mcp_registry"
documentdb_username: str | None = None
documentdb_password: str | None = None
documentdb_use_tls: bool = True
documentdb_tls_ca_file: str = "/app/certs/global-bundle.pem"
documentdb_use_iam: bool = False
documentdb_replica_set: str | None = None
documentdb_read_preference: str = "secondaryPreferred"
documentdb_direct_connection: bool = False
documentdb_namespace: str = "default"     # NB: drives collection-suffix multi-tenancy
```

**Required Postgres additions** (mapped per P3 §1.2). Recommendation: keep `storage_backend` accepting one new value `"postgres"`, and group all Postgres knobs under a `postgres_*` prefix to mirror `documentdb_*`:

```python
postgres_dsn: str | None = None
    # libpq DSN: postgres://user:pass@host:5432/db?sslmode=require
    # If set, takes precedence over individual postgres_host/port/user/password fields.
postgres_host: str = "localhost"          # used when DSN unset
postgres_port: int = 5432
postgres_database: str = "mcp_registry"
postgres_username: str | None = None
postgres_password: str | None = None
postgres_namespace: str = "default"
    # Drives schema selection (CREATE SCHEMA mcp_<namespace>) — equivalent to
    # documentdb_namespace's collection-suffix role. SET search_path on every
    # connection out of the pool.
postgres_pool_min: int = 2                # asyncpg pool min_size
postgres_pool_max: int = 10               # asyncpg pool max_size
postgres_statement_timeout_ms: int = 30000
    # Sent as `statement_timeout` GUC on each connection acquire.
postgres_connect_timeout_seconds: int = 10
postgres_command_timeout_seconds: int = 30
postgres_ssl_mode: str = "prefer"         # disable | allow | prefer | require | verify-ca | verify-full
postgres_ssl_root_cert: str | None = None # path to CA bundle, mirrors documentdb_tls_ca_file
postgres_application_name: str = "mcp-registry"
```

Optional but worth exposing (debt-prevention):
```python
postgres_max_connection_lifetime_seconds: int = 1800   # asyncpg max_inactive_connection_lifetime
postgres_pgvector_lists: int = 100        # IVFFlat lists tuning (if used instead of HNSW)
postgres_pgvector_ef_search: int = 100    # mirror of vector_search_ef_search for HNSW
```

Reuse/repurpose existing fields:
- `vector_search_ef_search: int = 100` (`config.py:107`) — already exists; should be honored by the Postgres `pgvector` HNSW path the same way DocumentDB uses it for `$vectorSearch`.
- `embeddings_model_dimensions: int = 384` (`config.py:102`) — already drives the column dimension for any vector store.
- `EmbeddingConfig.index_name` (`config.py:638-649`) generates `f"mcp-embeddings-{dimensions}-{namespace}"`. The Postgres impl should use the same string as the table name (or schema-qualified table) to keep multi-tenant parity.

---

## §5 — Mongo-isms in the abstraction

These are the ABC surfaces where MongoDB semantics leak through the contract dict. Each row: leak → meaning → Postgres equivalent → risk level.

| # | API | Where defined | What it carries | Postgres equivalent | Risk |
|---|---|---|---|---|---|
| 1 | `find_with_filter(filter_dict)` | `interfaces.py:168-180` (Server), `:292-304` (Agent) | Raw Mongo filter dict — `$or`, `$regex`, `$exists`, `$ne`, `$in`, dot-paths like `metadata.agentcore_registry_id` | Translator: `MongoFilterToSQL(filter_dict) -> (where_sql, params)`. Whitelist allowed operators. Use `jsonb_path_exists`/`->>'field'` for nested paths. | **HIGH** — callers compose arbitrary filters. See `services/ans_service.py:65-67` (`{"ans_metadata": {"$exists": True, "$ne": None}}`) and `api/federation_routes.py:799-805` (`{"metadata.agentcore_registry_id": registry_id}`, `{"tags": "agentcore", "_id": {"$regex": "^/agents/agentcore-"}}`). |
| 2 | `update_field(path, field, value)` | `interfaces.py:148-165` (Server), `:272-289` (Agent) | `field` accepts dot notation: `"ans_metadata.status"`, `"ans_metadata.last_verified"` | Translate `a.b.c` → `jsonb_set(col, '{a,b,c}', $1::jsonb)`; for None, `col #- '{a,b,c}'`. Top-level fields stay regular columns. | **MED** — callers in `services/ans_service.py:86-87, 91, 137, 187, 221, 248`. Need to know which top-level "fields" are actual columns vs JSONB paths. |
| 3 | `aggregate(pipeline)` | `audit_repository.py:108-122` | Mongo aggregation pipelines: `$match`, `$group`, `$sum`, `$first`, `$unwind`, `$sort`, `$limit`, `$ifNull`, `$cond`, `$regexMatch` | Cannot be implemented generically. Callers in `registry/audit/routes.py:407-475` need rewriting — translate each pipeline to SQL `GROUP BY`/`COUNT()` queries. | **HIGH** — audit routes hard-code 5+ aggregation pipelines. Phase Β must inventory and rewrite. |
| 4 | `distinct(field, query=None)` | `audit_repository.py:90-106` | Returns sorted distinct string values; `field` may be dot-pathed (`"identity.username"`, `"mcp_server.name"`) | `SELECT DISTINCT (col->'identity'->>'username') FROM audit_events WHERE … ORDER BY 1` | LOW — straightforward translation. Callers: `audit/routes.py:303, 307`. |
| 5 | TTL indexes | `documentdb/backend_session_repository.py:74-79` (`expireAfterSeconds=3600`); `audit_repository.py:144-146`; `mongodb_log_handler.py:97-102` | Auto-delete docs whose `last_used_at` (or `timestamp`/`created_at`) is older than N seconds | Either (a) cron/PG_CRON `DELETE WHERE last_used_at < now() - interval '...'`, or (b) partial expression index on `expires_at` + scheduled vacuum. Postgres has no native TTL; document the approach explicitly. | **MED** — silently affects correctness if not implemented. Three TTL collections (backend_sessions @ 1h, audit_events @ 7d via `audit_log_mongodb_ttl_days`, application_logs @ 1d via `app_log_centralized_ttl_days`). |
| 6 | `_id` as primary key | every `documentdb/*_repository.py` | Doc id is the path string (`"_id": path`); pop-and-rename idiom on read (`server_repository.py:67-68`, `agent_repository.py:53-55`) | SQL: `path TEXT PRIMARY KEY`. Add `path` column in `RETURNING *`; no pop-rename needed but `to_dict()` shape must match what callers expect (they read `server_info["path"]`). | LOW |
| 7 | `replace_one(filter, doc, upsert=True)` | `documentdb/search_repository.py:638, 708, 819, 949` and `backend_session_repository.py:156-160` | Atomic upsert that replaces entire doc | `INSERT … ON CONFLICT (path) DO UPDATE SET …` covering all columns. | LOW |
| 8 | `find_one_and_update(..., {"$set": ...})` | `documentdb/skill_repository.py:253-255`, `backend_session_repository.py:115, 231` | Atomic read-modify-write returning new doc | `UPDATE … SET … WHERE … RETURNING *` (single statement). | LOW |
| 9 | `$inc` atomic counter | `stats_repository.py:121-128` | `{"$inc": {"hourly.semantic_search_ctr": 1, "daily…": 1, "forever…": 1}}` | `UPDATE counters SET hourly = hourly+1, daily = daily+1, forever = forever+1 WHERE _id='counters'` (single row) — no transaction needed. | LOW |
| 10 | `$regex` (case-insensitive) on tags / paths | `documentdb/search_repository.py:987` (`{"tags": {"$regex": f"^{re.escape(tag)}$", "$options": "i"}}`); `documentdb/server_repository.py:289` (`{"_id": {"$regex": f"^{path}:"}}`) | Anchored regex on string columns | `WHERE tags ILIKE $1` (with `$1 = 'sometag'`) or `tags ~* '^sometag$'`. For the path-prefix case, `path LIKE 'path:%'` is exact. | LOW |
| 11 | `$exists`, `$ne`, `$in`, `$nin`, `$or`, `$and` operators | `documentdb/search_repository.py:215-274` (status filter); inferred from external callsites | Document-shape conditional logic | Translator table — `{"$exists": True}` → `IS NOT NULL`, `{"$ne": null}` → `!= NULL/IS NOT NULL`, `{"$in": [...]}` → `= ANY($1)`, `$or` → `OR`, `$and` → `AND`. | MED |
| 12 | `count_documents(filter_dict)` & `estimated_document_count()` | `app_log_repository.py:77-79`, every documentdb repo | Exact vs cheap-estimated counts | `SELECT COUNT(*) WHERE …` vs `pg_class.reltuples` for cheap estimate. | LOW |
| 13 | Pagination via `cursor.skip(offset).limit(limit)` | `app_log_repository.py:81`; every `list_paginated` impl | Implicit `_id` order assumed deterministic | `ORDER BY path ASC OFFSET $1 LIMIT $2`. Beware of `OFFSET` perf at high pages — out of scope for parity. | LOW |
| 14 | Naive datetime → UTC re-attach | `audit_repository.py:198-199, 230-232`; `stats_repository.py:103-106` | Motor returns naive datetimes that the code re-attaches `tzinfo=UTC` | asyncpg returns `datetime` with tzinfo when column is `TIMESTAMPTZ`. Always use `TIMESTAMPTZ`, never `TIMESTAMP`. | MED — silent bug class if missed. |

The two leakiest contracts are `find_with_filter` and `aggregate`. Phase Β should decide whether the Postgres impl writes a Mongo-filter→SQL translator or whether the upstream ABCs are split into per-call narrower methods (e.g. `find_by_metadata_field`, `count_by_status_grouped`). The translator path preserves the current ABC; the narrowing path requires an upstream PR before the Postgres backend can land cleanly.

---

## §6 — Async story (Motor → asyncpg)

**Driver:** `motor>=3.3.0` (`pyproject.toml:53`) wraps `pymongo>=4.6.0` (line 54). The async API surface used:
- `AsyncIOMotorClient` — connection pool (`documentdb/client.py:29-33`)
- `AsyncIOMotorDatabase` — db handle (line 34); `db[collection_name]` returns `AsyncIOMotorCollection`
- Async cursors: `async for doc in collection.find(…)` is the dominant iteration pattern (every `documentdb/*_repository.py`).

**Connection lifecycle:**
- Single global singleton `_client`/`_database` in `documentdb/client.py:12-13`. Lazy-created on first `get_documentdb_client()` call.
- `close_documentdb_client()` at line 42-48 zeros it.
- Connection options live in `mongodb_connection.py`: `build_connection_string()`, `build_client_options()`, `build_tls_kwargs()` (returns `{"tls": True, "tlsCAFile": …}`).
- Per-collection caching is done inside each repo: `self._collection: AsyncIOMotorCollection | None = None` then lazy `_get_collection()` (e.g. `documentdb/server_repository.py:19-28`).

**asyncpg port surface (recommended):**
1. Replace `_client/_database` with `_pool: asyncpg.Pool | None` in `postgres/client.py`. `await asyncpg.create_pool(dsn=..., min_size=postgres_pool_min, max_size=postgres_pool_max, command_timeout=postgres_command_timeout_seconds, server_settings={"application_name": ..., "statement_timeout": str(postgres_statement_timeout_ms)})`.
2. Replace `await get_documentdb_client()` with `async with pool.acquire() as conn:` inside each repo method (no per-collection caching equivalent — connection is the unit). Or pass `conn` through.
3. Replace `async for doc in cursor` with `async for record in conn.cursor("SELECT …", *args)` for streaming, or `await conn.fetch("…")` for bulk reads.
4. Replace `_get_collection()` with a no-op or remove entirely. Schema/table is fixed.
5. Replace `await close_documentdb_client()` with `await pool.close()`.

**Pydantic v2 model serialization** (used in 7+ repos):
- Encode: `model.model_dump(mode="json")` (e.g. `audit_repository.py:320`, `documentdb/agent_repository.py:116, 157, 703`, `documentdb/skill_repository.py:40`). With `mode="json"`, datetimes become ISO strings.
- Decode: `AgentCard(**doc)`, `SkillCard(**doc_copy)` (`documentdb/agent_repository.py:55, 71, 95`; `documentdb/skill_repository.py:75`).
- For Postgres, prefer `jsonb` columns receiving `model_dump(mode="json")` then re-hydrating via `Model(**json.loads(record["doc"]))`. Or split shape: keep top-level columns for indexed fields (`path`, `is_enabled`, `tags`, `registered_at`, `updated_at`) and dump the rest into a `data jsonb` column.
- Datetime helpers (`datetime.utcnow()` in `documentdb/server_repository.py:182, 214` — **deprecated in Py3.12+**) should be replaced with `datetime.now(UTC)` in the new Postgres repos.

**Error mapping:**
- `pymongo.errors.DuplicateKeyError` (used in `audit_repository.py:329`, `documentdb/agent_repository.py:127, server_repository.py:196`, `documentdb/skill_repository.py:235`) → `asyncpg.UniqueViolationError`. Repos must catch the asyncpg variant and produce equivalent service-level errors (`SkillAlreadyExistsError`, etc.).
- `pymongo.errors.OperationFailure` (`documentdb/search_repository.py:2039-2070`, used to detect MongoDB-CE missing vector support) → not directly applicable; asyncpg raises `asyncpg.PostgresError` subclasses.

---

## §7 — Vector search current behavior

`registry/repositories/documentdb/search_repository.py` (2,073 LoC) is the largest single file in the repo and the most concentrated source of platform-specific behaviour.

**Three modes** (all driven by the same `search()` entry point):

1. **DocumentDB native HNSW** (`initialize`, lines 525-540):
   ```python
   await collection.create_index(
       [("embedding", "vector")],
       name="embedding_vector_idx",
       vectorOptions={
           "type": "hnsw",
           "similarity": "cosine",
           "dimensions": settings.embeddings_model_dimensions,
           "m": 16,
           "efConstruction": 128,
       },
   )
   ```
   The hot path issues `$vectorSearch` aggregation stages with `efSearch: settings.vector_search_ef_search` (default 100, `config.py:107`).

2. **MongoDB-CE keyword fallback** (line 542-553): when `vectorOptions` is rejected, falls back to a regular B-tree index on `embedding` (which is useless for similarity but doesn't crash).

3. **Client-side cosine fallback** (`_client_side_search`, lines 1052-1378): when `$vectorSearch` raises `OperationFailure(code=31082)` (line 2041), the repo fetches **all** documents with `find(query_filter, {…projection})`, computes `_calculate_cosine_similarity` (lines 954-971) in Python, sorts by score, and returns. **This is the O(n) MongoDB-CE fallback** P3 §1.4 calls out — it works for ≤ a few hundred docs but doesn't scale.

**Hybrid scoring math** (lines 1185-1192 / 1850-1856):
```python
normalized_vector_score = (vector_score + 1.0) / 2.0     # cosine -1..1 → 0..1
text_boost_contribution = text_boost * 0.1               # weights below
relevance_score = normalized_vector_score + text_boost_contribution
relevance_score = max(0.0, min(1.0, relevance_score))
```
Text-boost weights (constant `MAX_LEXICAL_BOOST = 13.5`, line 110):
- path match: +5.0
- name match: +3.0
- description match: +2.0
- tag match: +1.5
- metadata_text match: +1.0
- per-tool match: +1.0

Plus a "soft cap" distribution (line 134-208) that prevents any single `entity_type` from claiming more than `SOFT_CAP_RATIO=0.6` of slots when other types have results competing.

**pgvector replacement contract:**
- Embeddings column type: `vector({embeddings_model_dimensions})` (extension `vector` from `pgvector`).
- Index: `CREATE INDEX ON {table} USING hnsw (embedding vector_cosine_ops) WITH (m=16, ef_construction=128);` — exact parity with the DocumentDB params.
- Query-time tuning: `SET LOCAL hnsw.ef_search = {vector_search_ef_search}` before each search query.
- Top-K query: `SELECT *, 1 - (embedding <=> $1::vector) AS vector_score FROM {table} WHERE … ORDER BY embedding <=> $1::vector LIMIT $2`.
- Lexical hybrid: keep the same text-boost formula, computed in Python after the SQL fetch (mirror `_client_side_search`'s post-processing) — or move into SQL via `tsvector`/`pg_trgm` if Phase Γ wants to push it down.
- The O(n) fallback **disappears entirely** with pgvector — Postgres always supports HNSW once the extension is loaded. Document this as a hard requirement: `CREATE EXTENSION IF NOT EXISTS vector` in the schema bootstrap.

Other vector-search shapes to preserve:
- `index_server` / `index_agent` / `index_skill` / `index_virtual_server` (lines 567-952): each builds a denormalized search row containing `embedding`, `metadata`, `text_for_embedding`, `embedding_metadata` (provider/model/dim, see `config.py:651-685`). One Postgres table can hold all entity types with an `entity_type TEXT` discriminator column, mirroring the current single-collection design.
- `get_all_tags` (lines 1019-1034): `$unwind` + `$group` + `$toLower`. Postgres equivalent: `SELECT DISTINCT lower(t) FROM {table}, unnest(tags) AS t ORDER BY 1`.

---

## §8 — Tests + CI

**Test layout** (`tests/`):
```
tests/
├── conftest.py                   ← root: SSL stubs, env setup, autouse mock_all_repositories
├── auth_server/                  ← separate suite for the auth-server side
├── fixtures/                     ← factories, mocks for FAISS/embeddings/litellm
├── integration/                  (12 files; mostly @pytest.mark.skip on MongoDB)
│   ├── conftest.py               ← reset_mongodb_client autouse, mock_security_scanner autouse
│   ├── test_mongodb_connectivity.py   ← pytest.mark.skip "Requires MongoDB running"
│   ├── test_search_integration.py     ← pytestmark = pytest.mark.skip (entire file)
│   ├── test_server_lifecycle.py
│   ├── test_skill_api.py
│   ├── test_skill_scanner_repository.py
│   ├── test_virtual_server_api.py
│   ├── test_peer_federation_e2e.py
│   ├── test_agentcore_sync_integration.py
│   ├── test_telemetry_e2e.py
│   └── test_deployment_mode_integration.py
├── unit/
│   ├── conftest.py
│   ├── repositories/             (4 files — only file-based + mocks, no live DB)
│   │   ├── test_file_server_repository.py
│   │   ├── test_registry_card_repository.py
│   │   ├── test_search_result_distribution.py
│   │   └── test_app_log_repository.py
│   └── …
└── security/
```

**Critical autouse fixture** — `tests/conftest.py:455-525` `mock_all_repositories` patches *every* `factory.get_*_repository` call to return `AsyncMock` instances. This is the universal isolation mechanism: all unit tests run with the file/file-mock backend regardless of `STORAGE_BACKEND`. Postgres parity for testing means:
- **No new fixture is needed for unit tests** — they already mock at the factory boundary.
- For Postgres-specific repository tests, add a new `tests/unit/repositories/test_postgres_*_repository.py` that mocks `asyncpg.Pool` (no live DB).
- For integration tests, follow the `test_mongodb_connectivity.py` pattern: skip-by-default, opt-in with `@pytest.mark.skip(reason="Requires Postgres running …")` and a corresponding workflow that spins up a postgres service container.

**`pytest_configure`** (`conftest.py:62-119`) sets env vars *before* Settings is imported:
- `DOCUMENTDB_HOST=localhost`, `DOCUMENTDB_PORT=27017`
- `STORAGE_BACKEND=mongodb-ce`
- `DOCUMENTDB_DIRECT_CONNECTION=true`, `DOCUMENTDB_USE_TLS=false`

For Postgres, parallel additions: `POSTGRES_HOST=localhost`, `POSTGRES_PORT=5432`, `POSTGRES_USERNAME=mcp`, `POSTGRES_PASSWORD=mcp`, `POSTGRES_DATABASE=mcp_registry_test`, with `STORAGE_BACKEND` driven by the test class (parametrize via `monkeypatch.setenv` per-test or a fixture).

**CI workflow** — `.github/workflows/registry-test.yml`:
- Three jobs: `test` (Python 3.14, `uv sync --extra dev`, `python scripts/test.py coverage -n 8`), `lint` (ruff), `security` (bandit). 30-min timeout.
- **No Postgres or MongoDB service container** is provisioned — all tests that need a live DB are `@pytest.mark.skip`'d. Coverage threshold: `--cov-fail-under=35` (`pyproject.toml:98`).
- For Postgres parity: a service block needs to be added:
  ```yaml
  services:
    postgres:
      image: postgres:17
      env: { POSTGRES_USER: mcp, POSTGRES_PASSWORD: mcp, POSTGRES_DB: mcp_registry_test }
      ports: ["5432:5432"]
      options: --health-cmd pg_isready --health-interval 10s
  ```
  Either alongside the existing matrix, or as a separate `integration-postgres` job that runs only the postgres-marked tests and unskips them.

**Reality check:** the upstream test suite barely exercises live MongoDB today — most integration tests are `pytest.mark.skip`'d (e.g. `test_search_integration.py:25-27`, every method in `test_mongodb_connectivity.py`). The Postgres branch should not lower this bar; it should *raise* it by introducing real service-container integration tests for at least `server_repository`, `agent_repository`, `search_repository`, and `audit_repository` against a Postgres+pgvector container.

---

## Appendix A — Summary of changes Postgres backend must make

| Layer | File | Change |
|---|---|---|
| Settings | `core/config.py` | +12 `postgres_*` fields (§4); accept `"postgres"` as `storage_backend` value |
| Factory | `repositories/factory.py` | Add 12 `elif backend == "postgres"` branches; close-pool hook |
| Stats | `repositories/stats_repository.py` | Add 3 postgres helpers; widen branch in 3 places |
| Connection util | `repositories/postgres/client.py` (new) | asyncpg pool singleton + `acquire()` ctx + `get_schema_name(base)` mirror of `get_collection_name` |
| Repository impls | `repositories/postgres/*.py` (12 new files) | One per ABC (server, agent, scope, security_scan, skill_security_scan, search, peer_federation, federation_config, skill, backend_session, virtual_server, registry_card, audit) |
| Filter translator | `repositories/postgres/filter.py` (new) | `MongoFilterToSQL` for `find_with_filter` (§5 row 1) |
| Schema bootstrap | `repositories/postgres/schema.sql` (new) | `CREATE EXTENSION vector;` + 14 `CREATE TABLE` + indexes (incl. HNSW + TTL-replacement) |
| Audit aggregations | `audit/routes.py` | Rewrite each `repository.aggregate(pipeline)` callsite to dispatch on backend (or push down behind a higher-level method) |
| Tests | `tests/unit/repositories/postgres/*` (new) | 12 asyncpg-mocked test modules |
| CI | `.github/workflows/registry-test.yml` | Add `services.postgres` block + integration-postgres job |
| Dependency | `pyproject.toml` | `asyncpg>=0.30.0`, `pgvector>=0.3.0` |

The 12-files-per-impl figure assumes `RegistryCardRepository` and `AppLogRepository` are added to Postgres (parity); strict minimum is 11 (drop `app_log_repository.py` parity).

— END OF MEMO —
