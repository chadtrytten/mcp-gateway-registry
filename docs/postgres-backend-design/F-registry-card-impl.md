# POSTGRES-F — `PostgresRegistryCardRepository` proof-of-concept

**Author:** opus subagent POSTGRES-F (CCLI2 batch-NEXT-27 Phase A) · **Date:** 2026-04-29

**Companion artifacts**
- `outputs/postgres-F-RegistryCardRepository.py` — ready-to-fork implementation (202 LOC)
- `outputs/postgres-F-test_registry_card_repository.py` — pytest scaffolding (256 LOC, 7 tests)

**Upstream targets**
- `registry/repositories/postgres/registry_card_repository.py` (new)
- `tests/integration/test_postgres_registry_card_repository.py` (new)

---

## §1 — ABC fidelity note (important)

The instruction prompt listed `get / set / update_field / exists / clear`. The
actual `RegistryCardRepositoryBase` ABC at `registry/repositories/interfaces.py:1460-1480`
declares **three** abstract methods: `get()`, `save(card)`, `exists()`. The DocumentDB
impl at `documentdb/registry_card_repository.py` implements exactly those three. The
implementation matches the actual ABC, satisfying the explicit "Match
RegistryCardRepositoryBase ABC contract exactly" line of the prompt.

If a future scope-bump adds `update_field`/`clear`, the JSONB pattern documented
in P3 §3.2 ports verbatim.

## §2 — Schema (mirrors P3 §2 conventions)

```sql
CREATE TABLE IF NOT EXISTS registry_cards_default (
    id          TEXT PRIMARY KEY,           -- always 'default'
    data        JSONB NOT NULL,             -- Pydantic dump sans timestamps
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS registry_cards_default_data_gin
    ON registry_cards_default USING GIN (data jsonb_path_ops);

CREATE TRIGGER registry_cards_default_updated_at
    BEFORE UPDATE ON registry_cards_default
    FOR EACH ROW EXECUTE FUNCTION mcp_set_updated_at();
```

`_default` suffix is the namespace from `table_name("registry_cards")` (POSTGRES-D).

**Why timestamps in columns rather than embedded JSONB?** Trigger guarantees
`updated_at` advances on UPDATE without app-side logic; column default keeps
`created_at` stable across `ON CONFLICT DO UPDATE`. The JSONB body is reconstituted
with the timestamps on read (§3.1) so the rehydrated `RegistryCard` matches
DocumentDB's surface.

## §3 — Method-by-method

### 3.1 `get()` — timestamp-synthesis read

```sql
SELECT jsonb_set(
           jsonb_set(data, '{created_at}', to_jsonb(created_at)),
           '{updated_at}', to_jsonb(updated_at)
       ) AS data
FROM registry_cards_default WHERE id = 'default';
```

`to_jsonb(timestamptz)` → RFC-3339 string, accepted by Pydantic v2's datetime
parser. Connection / unexpected / Pydantic-validation errors all logged + return
`None` (matches DocumentDB contract for federation-discovery surfaces).

### 3.2 `save(card)` — single-statement upsert

```sql
INSERT INTO registry_cards_default (id, data) VALUES ($1, $2::jsonb)
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
```

Pydantic dump is stripped of `created_at`/`updated_at` (DB owns them) before
encoding. One round-trip vs DocumentDB's two (find_one + replace_one). Wrapped
in `conn.transaction()` so behaviour stays correct under PgBouncer transaction
mode later. Connection / integrity errors logged + **raised** — writes are loud.

### 3.3 `exists()` — narrow probe

`SELECT 1 FROM … WHERE id = 'default' LIMIT 1`. Connection errors logged + return
`False` (used by `/health`-style endpoints; must not throw).

## §4 — Error-handling contract

| Site | `PostgresConnectionError`/`OSError` | `IntegrityConstraintViolation` | Pydantic / unexpected |
|---|---|---|---|
| `get()`     | log + `None`    | n/a              | log + `None`    |
| `save()`    | log + **raise** | log + **raise**  | log + **raise** |
| `exists()`  | log + `False`   | n/a              | log + `False`   |

Reads best-effort, writes loud — matches DocumentDB exactly so the abstraction
stays uniform across backends.

`NotImplementedError` for unsupported Mongo filter ops is **not relevant**
here — the ABC has no `find_with_filter` for the singleton card. The
POSTGRES-E translator gets exercised first by `Server`/`Agent`/`Scope`.

## §5 — Dependencies on POSTGRES-D's `client.py`

```python
from .client import get_pool, table_name
```

The implementation expects:

1. `async def get_pool() -> asyncpg.Pool` — singleton pool from
   `settings.postgres_dsn`.
2. `def table_name(base: str) -> str` — appends namespace suffix.
3. Pool init registers a JSONB codec
   (`set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads)`). The
   implementation is **defensive** if it doesn't:
   `if isinstance(data, str): data = json.loads(data)`.

If POSTGRES-D's API names diverge, two import lines need updating.

## §6 — Tests

Seven tests (one per ABC method + upsert / failure edges) following upstream's
skip-in-CI integration pattern (P3 §7.2 Path A; same shape as
`tests/integration/test_mongodb_connectivity.py:17,35`):

| # | Test | Covers |
|---|---|---|
| 1 | `test_get_returns_none_when_table_empty` | empty path |
| 2 | `test_save_inserts_then_get_round_trips_full_payload` | full Pydantic round-trip |
| 3 | `test_save_upsert_preserves_created_at_and_advances_updated_at` | trigger + ON CONFLICT |
| 4 | `test_exists_false_then_true` | `exists()` |
| 5 | `test_get_with_corrupt_row_returns_none` | Pydantic validation on read |
| 6 | `test_save_raises_on_dead_pool` | write-path error contract |
| 7 | `test_get_with_dead_pool_returns_none` | read-path error contract |

**Isolation:** each test gets a randomly-named Postgres schema
(`test_pgrcr_<uuid>`) created on fixture setup, dropped CASCADE on teardown.
Fixture monkey-patches `table_name` to point at the scoped schema.
Parallel-safe under `pytest-xdist`.

**Skip gate:** `pytest.mark.skipif(POSTGRES_TEST_DSN is None, …)` — matches the
existing skip-in-CI convention. testcontainers fixture is sketched (commented
out) at the bottom of the file; lands in a separate PR per P3 §7.2.

## §7 — Reusable patterns established

The 13 remaining repositories inherit:

1. **Lazy pool acquisition** via `_pool()` indirection.
2. **JSONB-only payload + hot columns** (RegistryCard demonstrates the *minimum*
   form — no hot scalars; later repos add generated columns for `is_enabled`,
   `source`, `status` etc).
3. **DB-owned timestamps** via trigger + column default + `jsonb_set` synthesis.
4. **Asymmetric error contract** (read swallows, write raises).
5. **Defensive JSONB codec handling** (`isinstance(data, str)` guard).
6. **Schema-scoped test isolation** with `monkeypatch` of `table_name`.

The next code-author implementing `PostgresServerRepository` (13 methods)
follows the same shape plus hot-column extraction, `delete_with_versions`
LIKE-prefix DELETE, and the POSTGRES-E filter translator for
`find_with_filter`.

## §8 — Open items

1. **POSTGRES-D import names.** Two import lines (`get_pool`, `table_name`)
   may need adjustment when POSTGRES-D lands.
2. **POSTGRES-B canonical DDL.** The DDL inlined as `TABLE_DDL`/`TRIGGER_DDL`
   in the impl file is for test-fixture self-containment. If POSTGRES-B's
   `postgres-B-tables-013-registry-cards.sql` diverges, trust POSTGRES-B for
   migrations and update these strings for fixture parity only.
3. **Factory wiring** — out of scope for this file; lands in PR #1 of the
   staged plan (P3 §8.1) alongside settings:
   ```python
   if backend == "postgres":
       from .postgres.registry_card_repository import PostgresRegistryCardRepository
       _registry_card_repo = PostgresRegistryCardRepository()
   ```

---

**End of memo. Implementation + tests are ready-to-fork pending POSTGRES-D's
`client.py` API stabilising.**
