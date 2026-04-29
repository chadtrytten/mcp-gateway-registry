# POSTGRES-E — Mongo-filter → SQL translator (8-op subset)

**Author:** agent-POSTGRES-E (Opus subagent)
**Date:** 2026-04-29
**Scope:** design + reference impl + tests; no commits.
**Companion file:** [`postgres-E-mongo-filter-py.py`](./postgres-E-mongo-filter-py.py) — runnable module, 27/27 tests passing.

## 1. Upstream callsite audit

`gh search code 'find_with_filter' org:agentic-community` returned ten hits; after deduplicating definitions vs. invocations, **only three concrete callsites** drive the operator-coverage requirement, all in `agentic-community/mcp-gateway-registry@main`:

| # | Callsite | Filter passed | Operators exercised |
|---|---|---|---|
| 1 | `registry/api/federation_routes.py:799` | `{"metadata.agentcore_registry_id": registry_id}` | nested-dot equality |
| 2 | `registry/api/federation_routes.py:803-805` | `{"tags": "agentcore", "_id": {"$regex": "^/agents/agentcore-"}}` | top-level equality, **`$regex`**, implicit AND of sibling keys, `_id` mapping |
| 3 | `registry/services/ans_service.py:65-67` | `{"ans_metadata": {"$exists": True, "$ne": None}}` | **`$exists:true` + `$ne:null` combined in one operator-dict** (implicit AND inside the dict) |

The interfaces (`registry/repositories/interfaces.py:168, 292`) and the four backend implementations (`file/{agent,server}_repository.py`, `documentdb/{agent,server}_repository.py`) are definitions, not call sites.

`$or`/`$and`/`$in` do **not** appear in any current `find_with_filter(...)` invocation — but they do appear elsewhere in the codebase (e.g. `audit/routes.py`, `repositories/documentdb/search_repository.py`, `core/telemetry.py`) on direct `collection.find(...)` calls or on filter dicts that are subsequently extended. Because those code paths are likely to migrate onto `find_with_filter` (or its equivalent abstraction) as the Postgres backend lands, **the 8-op spec from P3 §3.6 is the right floor**: implementing only the 3-op working set today would leave the next caller stuck.

**Verdict:** the 8-op spec is necessary and sufficient. No new operators identified.

## 2. Operator → SQL emission

| Mongo expression | SQL fragment | Param(s) | Notes |
|---|---|---|---|
| `{"f": v}` (top, scalar) | `data->>'f' = $n` | text-coerced `v` | `bool→'true'/'false'`, `int/float→str(...)` so equality matches PG `->>` text output |
| `{"f": v}` (nested `a.b`) | `data #>> '{a,b}' = $n` | text-coerced `v` | dot-notation split on `.`, each part validated as `[A-Za-z_][A-Za-z0-9_]*` |
| `{"_id": v}` | `path = $n` | `v` | `_id` is the configurable id-column (POSTGRES-B schema); default `path`, settable per call |
| `{"f": None}` | `data->>'f' IS NULL` | – | matches missing keys *and* JSON-null, mirroring Mongo |
| `{"f": {"$exists": true}}` (top) | `data ? 'f'` | – | uses GIN-indexable `?` |
| `{"f": {"$exists": true}}` (nested) | `data #> '{a,b}' IS NOT NULL` | – | falls through `#>` to detect missing parents |
| `{"f": {"$exists": false}}` | `NOT (data ? 'f')` | – | inverted form, same rule for nested |
| `{"f": {"$ne": v}}` | `data->>'f' IS DISTINCT FROM $n` | text-coerced `v` | NULL-safe |
| `{"f": {"$ne": null}}` | `data->>'f' IS NOT NULL` | – | matches Mongo "field present and not null" |
| `{"f": {"$in": [...]}}` | `data->>'f' = ANY($n::text[])` | list of coerced values | empty list → `FALSE`; mixed-with-`null` → splits into `IS NULL OR = ANY(...)` |
| `{"f": {"$regex": p}}` | `data->>'f' ~ $n` | `p` | Postgres POSIX regex; PCRE-only patterns are caller's responsibility |
| `{"$or":  [d1,d2,...]}` | `(<d1> OR <d2>...)` | – | recurses |
| `{"$and": [d1,d2,...]}` | `(<d1> AND <d2>...)` | – | recurses |
| sibling top-level keys | implicit `AND` between clauses | – | matches Mongo |
| sibling op-dict keys | implicit `AND` between op-clauses | – | matches Mongo (e.g. callsite #3) |

Anything else (`$gt`, `$lt`, `$where`, `$elemMatch`, literal-object equality, …) raises `TranslationError` (subclass of `NotImplementedError`) with the offending operator in the message.

## 3. Module API

```python
def translate(
    filter_dict: dict[str, Any],
    *,
    id_column: str = "path",
    data_column: str = "data",
    start_param: int = 1,
) -> tuple[str, list[Any]]: ...
```

Returns `(sql_fragment, params)`. The fragment can be appended directly after `WHERE`. Params are bound asyncpg-style (`$1`, `$2`, …); `start_param` lets the caller offset the numbering when composing fragments. An empty filter returns `("TRUE", [])` so `WHERE TRUE` is always valid.

For repeated translation in a hot path, use the `MongoToPostgresTranslator` class directly — `translate()` is a thin one-shot wrapper.

## 4. Safety considerations

1. **JSONB path injection.** The `#>>'{a,b}'` literal cannot be parameterized at the path level, so each dot-separated part is validated against a strict identifier regex `^[A-Za-z_][A-Za-z0-9_]*$`. Field names in the upstream codebase are baked into call sites (never user-supplied), but the validation is defense-in-depth.
2. **Column-name injection.** `id_column` and `data_column` arguments are validated with the same regex.
3. **Value injection.** Every value flows through a numbered placeholder (`$n`); no value is ever interpolated into SQL text.
4. **Whitelist-only operator dispatch.** The dispatcher rejects any `$`-prefixed key not in the 8-op set, both at the dict level (`$or`, `$and`) and at the field level (`$exists`, `$ne`, `$in`, `$regex`).
5. **No silent semantic drift.** Cases where Mongo and Postgres diverge are handled explicitly:
   - `$ne: null`  → `IS NOT NULL`, not `IS DISTINCT FROM 'null'`.
   - `$in: []`    → `FALSE` (matches nothing), not parameter-error.
   - `$in` with a `null` element → split into `IS NULL OR = ANY(...)`, because Postgres `ANY` ignores NULL members.
   - Bare nested-dict on a field (e.g. `{"f": {"a": 1}}`) is rejected, not converted to JSON-equality, since the documented operator set is text-based.

## 5. Test coverage

27 tests in the same file, runnable via `python postgres-E-mongo-filter-py.py` (zero deps; pytest-compatible — pytest can collect the same `test_*` functions):

- **Per-operator (12):** equality (top + nested), `$exists` true/false (top + nested), `$ne` value, `$ne` null, `$in` list, `$in` empty, `$regex`, `$or`, `$and`.
- **Real callsite reproductions (3):** federation metadata, federation tags + `_id` regex, ans_service combined `$exists`+`$ne:null`.
- **Composition (4):** nested `$or`/`$and`, `start_param` offset, empty filter, equality-to-null.
- **Safety / whitelist (8):** unsupported operator, top-level unknown `$`, unsafe field name, mixed op+literal keys, unsupported value type, custom columns, `$in`-with-null splitting, numeric-as-text encoding.

Output of test run: `27/27 passed`.

## 6. Open questions for downstream tasks

1. **`_id` column name** — defaulted to `"path"` to match the existing Mongo pattern (`results[doc_id] = doc`, where `doc_id` is the path). POSTGRES-B (schema DDL) should confirm. If the table uses a different PK name (e.g. `id`), callers pass `id_column="id"`.
2. **Indexing** — `data ? 'field'` and `data->>'field' = ...` are GIN-indexable with the right `jsonb_path_ops` opclass. Out of scope here, but POSTGRES-B should add expression indexes for the hot fields surfaced in the audit (`tags`, `metadata.agentcore_registry_id`, `ans_metadata`, the `path` regex).
3. **Regex flavor** — Postgres `~` is POSIX, not PCRE. The two callsites in scope use anchored prefix patterns (`^/agents/agentcore-`) which work identically. If a future caller passes a PCRE-only construct (e.g. `(?<lookbehind>)`), `~` will error at execute time. Worth a follow-up note in POSTGRES-A's contract memo.
4. **Param style** — emitted as `$n` (asyncpg). If POSTGRES-D picks psycopg/sqlalchemy, a 2-line post-processor maps `$n` → `%s` while preserving the param-list order.
