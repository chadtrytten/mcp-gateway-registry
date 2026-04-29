# POSTGRES-D — Connection management design memo

**Author:** opus subagent POSTGRES-D (CCLI2 batch-NEXT-27, Phase A)
**Date:** 2026-04-29
**Scope:** `registry/repositories/postgres/client.py` — pool, retry, health, TLS, flock guidance.
**Companion artifact:** `postgres-D-client-py-skeleton.py` — ready-to-fork module.
**Inputs:** P3 §4 (connection mgmt + asyncpg), P3 §1.3 (upstream singleton pattern at `documentdb/client.py:12-13`), P3 §1.2 (settings parity), W2/R1/lib.sh flock prior art.

---

## 1. Goals & non-goals

**Goals**

1. Mirror upstream's `documentdb/client.py:12-13` singleton lifecycle so the Postgres backend slots into the existing factory pattern at `registry/repositories/factory.py` with minimal review-surface.
2. Single source of truth for pool acquisition (`get_pool()`), pool teardown (`close_pool()`), per-connection setup (`_setup_connection`), table-name namespacing (`_table()`), and health probe (`health_check()`).
3. Production-quality defaults: bounded pool, statement timeout, vector codec registration, retry on transient transport errors, opt-in TLS.
4. Zero new heavy deps. Reuse `tenacity` (already in upstream pyproject for federation retries).

**Non-goals (Phase A)**

- PgBouncer / external pooler integration. P3 §4.4 deferred to v2 follow-up. Document the `statement_cache_size=0` knob, do not enable.
- Migration runner. POSTGRES-B owns DDL + migrations; the client only needs to assume the extension and tables already exist.
- ORM. Repositories use raw asyncpg per P3 §4.1.
- Per-query distributed locking. Postgres MVCC + advisory locks cover that. flock guidance below is for *host-level* bulk-import scripts, not the asyncpg path.

---

## 2. Module layout

```
registry/repositories/postgres/
├── __init__.py
├── client.py           ← THIS DESIGN
├── migrations/
│   └── 001_initial.sql ← POSTGRES-B
└── *_repository.py     ← POSTGRES-E/F/etc.
```

`client.py` is the only module that talks to `asyncpg.create_pool`. Repositories MUST go through `get_pool()` and the `_table(name)` helper — no direct DSN usage.

---

## 3. Singleton pool (mirrors upstream)

Upstream pattern at `documentdb/client.py:12-13`:

```python
_client: AsyncIOMotorClient | None = None

async def get_client() -> AsyncIOMotorClient: ...
async def close_client() -> None: ...
```

Postgres analog:

```python
_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool: ...
async def close_pool() -> None: ...
```

**Lifecycle invariants**

- `_pool` is module-level state; it is **process-singleton**, not request-scoped. asyncpg's `Pool` is asyncio-safe; reuse across coroutines is correct.
- Lazy init on first `get_pool()` call — matches upstream's lazy `get_client()`. Avoids requiring app startup to know whether Postgres is the active backend.
- `close_pool()` is idempotent: safe to call from FastAPI `shutdown` hook even if the pool was never created.
- A `_pool_lock = asyncio.Lock()` guards the lazy-init race when concurrent first calls land. Upstream's mongo client doesn't need this because Motor's `AsyncIOMotorClient()` is sync and cheap; `asyncpg.create_pool` is `async` and not. Without the lock you can briefly create two pools and leak one.

---

## 4. Per-connection setup (`init` callback)

asyncpg's `create_pool(init=...)` runs the callback once per physical connection (on creation, and again on reconnect). Use it for:

1. **Vector codec registration** — `await pgvector.asyncpg.register_vector(conn)`. Mandatory for `vector` columns to round-trip as numpy arrays / lists. Without this, asyncpg raises `cannot encode vector`.
2. **Statement timeout** — `SET statement_timeout = $POSTGRES_STATEMENT_TIMEOUT_MS` (default 30000 ms). Enforces a per-statement guillotine inside Postgres. Independent of asyncpg's `command_timeout` (which kills at the client side).
3. **Application name** (recommended, low cost) — `SET application_name = 'mcp-gateway-registry'`. Surfaces in `pg_stat_activity` for ops debugging.

Per-connection `SET` (without `LOCAL`) persists for the connection's lifetime in the pool. Don't issue `SET LOCAL` here — that's transaction-scoped.

**Failure mode:** if `init` raises, asyncpg fails the `create_pool()` call. Surface a clear error: missing `vector` extension, bad DSN, etc.

---

## 5. Configurable knobs (settings)

Per P3 §1.2 parity with `documentdb_*` settings, add to `registry/core/config.py`:

| Setting | Env | Default | Purpose |
|---|---|---|---|
| `postgres_dsn` | `POSTGRES_DSN` | *(unset)* | Full libpq DSN; if set wins over split fields. |
| `postgres_host` | `POSTGRES_HOST` | `localhost` | Host (used iff DSN unset). |
| `postgres_port` | `POSTGRES_PORT` | `5432` | |
| `postgres_database` | `POSTGRES_DATABASE` | `mcp_registry` | |
| `postgres_username` | `POSTGRES_USERNAME` | `postgres` | |
| `postgres_password` | `POSTGRES_PASSWORD` | *(unset, secret)* | |
| `postgres_namespace` | `POSTGRES_NAMESPACE` | `default` | Multi-tenant suffix; mirrors `documentdb_namespace`. |
| `postgres_pool_min` | `POSTGRES_POOL_MIN` | `2` | Pool floor. |
| `postgres_pool_max` | `POSTGRES_POOL_MAX` | `10` | Pool ceiling. |
| `postgres_command_timeout_s` | `POSTGRES_COMMAND_TIMEOUT_S` | `60.0` | asyncpg client-side per-command timeout. |
| `postgres_statement_timeout_ms` | `POSTGRES_STATEMENT_TIMEOUT_MS` | `30000` | Server-side statement timeout. `0` disables. |
| `postgres_ssl_mode` | `POSTGRES_SSL_MODE` | `prefer` | One of `disable / allow / prefer / require / verify-ca / verify-full`. |
| `postgres_ssl_ca_file` | `POSTGRES_SSL_CA_FILE` | *(unset)* | Path to CA bundle. Required for `verify-ca`/`verify-full`. |
| `postgres_statement_cache_size` | `POSTGRES_STATEMENT_CACHE_SIZE` | `100` | Set to `0` when fronting with PgBouncer transaction-mode (v2). |

Notes:

- `min_size=2` keeps two warm connections — sufficient for the upstream's autouse-mocked test world (P3 §1.5) and for liveness/readiness probes without provoking cold starts.
- `max_size=10` is the asyncpg default and matches the realistic single-replica gateway. Operators with peer-federation fanout can bump to 20-30 via env.
- `command_timeout=60s` upper bound; statement_timeout=30s lower bound. The two-layer timeout means the server kills the query first (clean), client kills the round-trip second (defensive).

---

## 6. TLS / SSL

asyncpg accepts `ssl=` as either a string mode name or an `ssl.SSLContext`. We use the libpq mode names so configuration matches `psql`/PgBouncer/RDS docs.

| Mode | Behavior |
|---|---|
| `disable` | No TLS. |
| `allow` | TLS only if server insists. |
| `prefer` *(default)* | TLS if server supports; fall back to plaintext. |
| `require` | TLS mandatory; **does not validate server cert**. |
| `verify-ca` | TLS mandatory; verify chain against CA bundle. |
| `verify-full` | `verify-ca` + hostname match. **Recommended for prod.** |

**Helper** (`_build_ssl(...)` in skeleton): for `verify-ca`/`verify-full` build an `ssl.SSLContext` with `load_verify_locations(cafile=settings.postgres_ssl_ca_file)`; otherwise pass the mode string directly.

**Trust-store setup (deployment doc, not code)**

- AWS RDS Postgres: download `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem` → mount at `/etc/ssl/certs/rds-global-bundle.pem` → `POSTGRES_SSL_MODE=verify-full`, `POSTGRES_SSL_CA_FILE=/etc/ssl/certs/rds-global-bundle.pem`.
- Self-managed Postgres: ship the deployment-CA's PEM through the same env vars.
- Local docker compose: `POSTGRES_SSL_MODE=disable` is acceptable; default `prefer` works because the `pgvector/pgvector:pg16` image ships TLS off.

Do NOT default to `disable`; default `prefer` so any TLS-enabled server gets used without explicit opt-in. Do NOT default to `require` — that breaks the docker compose path.

---

## 7. Retry policy

Per P3 §4.3, reuse `tenacity` (already in upstream deps for federation retries — see `pyproject.toml`).

**Decorator:** `@postgres_retry` applied at the **repository method** level, not at `pool.acquire`. Reasoning: a retry needs to re-acquire from the pool too, since the failed connection has been released back to the pool poisoned and asyncpg's `init` callback already handles reconnection of broken physical sockets.

```python
postgres_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type((
        asyncpg.PostgresConnectionError,
        asyncio.TimeoutError,
    )),
    wait=tenacity.wait_exponential_jitter(initial=0.1, max=2.0),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
```

**What we DON'T retry**

- `asyncpg.PostgresError` *subclasses other than connection*: `UniqueViolationError`, `CheckViolationError`, `DataError`, `SerializationFailure`, etc. These are deterministic and a retry just re-fails. Caller decides.
- `asyncpg.SerializationError`: only retriable inside an explicit `READ COMMITTED → REPEATABLE READ` upgrade pattern, which we don't use in v1. Out of scope.
- `asyncio.CancelledError`: never retry, propagate.

**Scope:** apply on read paths and idempotent writes. Non-idempotent writes (INSERT without ON CONFLICT) need either a transaction wrapper or an idempotency key — flag during repository review, not here.

---

## 8. Health probe

Match upstream's mongo health pattern (`registry/health/`). Repository method:

```python
async def health_check() -> dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        version = await asyncio.wait_for(
            conn.fetchval("SELECT version()"),
            timeout=2.0,
        )
    return {
        "status": "ok",
        "backend": "postgres",
        "version": version,
        "namespace": settings.postgres_namespace,
        "pool": {"size": pool.get_size(), "idle": pool.get_idle_size()},
    }
```

**Important:** the probe uses `asyncio.wait_for` with a **shorter** timeout (2s) than `command_timeout` (60s). A health check that blocks for 60s defeats the purpose of having one. The wait_for cancels the in-flight query at the asyncio level if Postgres is wedged.

Do NOT route health probes through `@postgres_retry`. A stuck-but-recoverable pool should still surface as "degraded" promptly to the orchestrator, not be hidden behind 3 retries × 2s = 7s of wait.

Wire into `/health` conditional on `STORAGE_BACKEND=postgres`, mirroring the existing mongo branch.

---

## 9. `_table()` namespacing helper

Upstream's `documentdb/client.py:51-55` (`get_collection_name`) suffixes every Mongo collection with `_{namespace}`. The Postgres equivalent suffixes table names:

```python
def _table(base_name: str) -> str:
    """Apply tenant namespace suffix. mcp_servers → mcp_servers_default."""
    return f"{base_name}_{settings.postgres_namespace}"
```

All repository SQL composes table names via `_table("mcp_servers")` etc. The function returns a plain identifier — callers use it inside f-strings on the SQL body, NOT as a `$1` parameter (asyncpg parameters are values, not identifiers).

**Validation:** `postgres_namespace` is settings-controlled (operator-supplied), but defensive regex `^[a-z][a-z0-9_]{0,30}$` should run once at settings load to prevent SQL injection via env. POSTGRES-B's DDL generator must match the same constraint.

---

## 10. flock guidance (W2 + R1 + lib.sh::_with_flock)

`lib.sh` uses `flock -n 9` on `${ENVIRODEX_LOGS_ROOT}/.harvest_logs.lock` etc. to prevent concurrent runs of the same shell script across cron + manual invocations (M2 memo, commit `5c04575`). That pattern is **host-level** mutual exclusion for shell processes.

**For the asyncpg path: do NOT use flock.**

- asyncpg pool acquisition + Postgres MVCC already serialize concurrent writers correctly.
- Concurrent reads scale linearly up to `max_size`; flock would serialize them and destroy throughput.
- Cross-process coordination, when needed, uses `pg_advisory_lock(key)` / `pg_advisory_xact_lock(key)` — Postgres-native, transactional, observable in `pg_locks`.

**Where flock IS appropriate (operational scripts, not the gateway runtime):**

| Use case | Lock file | Why |
|---|---|---|
| Bulk-import script that pipes a Mongo dump into Postgres | `/var/run/mcp-registry/.bulk-import.lock` | Re-running mid-stream would dual-INSERT. Combine with `INSERT ... ON CONFLICT DO NOTHING` for safety, but flock makes the intent explicit. |
| Manual reindex script (HNSW rebuild) | `/var/run/mcp-registry/.reindex.lock` | `REINDEX INDEX CONCURRENTLY` is safe to run alone; running two in parallel wastes I/O and contends shared buffers. |
| Migration runner (`_run_migrations()`) | `pg_advisory_lock(0xDEADBEEF)` *(NOT flock)* | The runner itself uses a Postgres advisory lock — works across hosts. flock wouldn't, since it's filesystem-local. |
| `mcp-cron` jobs that touch the DB | per-job advisory lock | Same as above. flock is fine if the job is single-host; advisory lock is fine if multi-host. |

**Decision rule:**

- Single host, single script, no DB transaction needed → flock (matches lib.sh idiom).
- Multi-host or "must be exactly-one across the cluster" → advisory lock (`SELECT pg_advisory_lock($key)`).
- Within a transaction → `pg_advisory_xact_lock` so the lock auto-releases on commit/rollback.

`client.py` exposes the advisory-lock helper (`async with advisory_lock(conn, key): ...`) for the migration runner and bulk operations. flock stays in shell-script territory and is documented in the operator runbook only.

---

## 11. Integration plan

**Phase A (this memo + POSTGRES-B/C/E/F):** module skeleton, DDL, pgvector config, filter translator, registry-card impl. No factory wiring yet — the upstream factory still routes `STORAGE_BACKEND=postgres` to a `NotImplementedError`.

**Phase B (subsequent batch):**

1. Add `pyproject.toml` deps: `asyncpg>=0.29.0`, `pgvector>=0.3.0`. Tenacity already present.
2. Add the settings table from §5 to `registry/core/config.py`.
3. Drop `client.py` skeleton in place at `registry/repositories/postgres/client.py`.
4. Wire `factory.py` getter for each repository to import the postgres impl when `storage_backend == "postgres"`.
5. FastAPI lifespan hook: `app.add_event_handler("shutdown", close_pool)`. Symmetric to how mongo does it.
6. `/health` route conditional branch.

**Test surface (matches P3 §1.5 — upstream skips integration tests):**

- Unit: factory routes `postgres` → postgres impls (no network).
- Integration (`@pytest.mark.skip(reason="Requires Postgres running...")` by default): bring up the `pgvector/pgvector:pg16` container, run migrations, exercise CRUD + vector search.
- The autouse fixture at `tests/conftest.py:455-525` already mocks every repository factory function — most of the existing test suite continues to pass with no changes.

---

## 12. Open questions / deferred

- **PgBouncer transaction-mode** (P3 §4.4): document only; `statement_cache_size=0` knob is exposed via setting so a v2 doc can flip it without code change.
- **Read replicas / replica routing**: out of scope for v1. Re-evaluate after the first prod deployment shows read load justifies it.
- **Connection retry storms**: tenacity caps at 3 attempts × 2s jitter ≈ 6s worst case per request. If the upstream gateway has stricter SLOs, swap to `stop_after_delay(5)`.
- **Metrics**: the skeleton exposes `pool.get_size()` / `get_idle_size()` via the health probe; a Prometheus exporter is a v2 task.

---

## 13. Acceptance — how to verify the skeleton matches this memo

The companion `postgres-D-client-py-skeleton.py` MUST contain, in order:

1. `_pool: asyncpg.Pool | None = None` + `_pool_lock: asyncio.Lock`
2. `async def _setup_connection(conn)` — vector codec + statement_timeout + application_name
3. `def _build_ssl(...)` — returns string or `SSLContext` per §6
4. `async def get_pool() -> asyncpg.Pool` — lazy + lock-guarded
5. `async def close_pool() -> None` — idempotent
6. `def _table(base_name: str) -> str` — namespace suffix
7. `postgres_retry` — tenacity decorator constant per §7
8. `async def health_check() -> dict` — short timeout per §8
9. `async def advisory_lock(conn, key)` — async context manager per §10
10. NO references to flock — flock guidance is documented in this memo only.

Reviewer checklist: each numbered item exists, signatures match, defaults match §5 table.
