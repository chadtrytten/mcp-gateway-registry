# POSTGRES-C — pgvector configuration + HNSW tuning

Design memo for the `mcp_embeddings_{N}` table on Postgres 16+. Achieves
behavioral parity with the upstream DocumentDB hybrid search at
`registry/repositories/documentdb/search_repository.py:480-540` (HNSW+cosine,
`m=16`, `efConstruction=128`, dim=1536, normalization `(cos + 1) / 2`).

Sibling memos: POSTGRES-A (abstraction contract), POSTGRES-B (DDL),
POSTGRES-D (connection mgmt), POSTGRES-E (filter translator),
POSTGRES-F (registry-card impl). All numerics here track the verified
upstream `vectorOptions` block at `search_repository.py:526-538`.

---

## §1 Extension install

pgvector 0.8.2 is the reference version (released 2026; we hard-pin to
`>=0.8.0` so iterative scans and parallel HNSW builds are available).

Install order, in the order we'll prefer:

1. **Official Docker image** (production + CI). `pgvector/pgvector:pg16-bookworm`
   ships PG 16 + pgvector compiled with SIMD already enabled. We point our
   `compose.yaml` and managed-PG sidecar at this tag; it's the only path that
   gives bit-identical behavior across dev/stage/prod.
2. **APT** (when bringing pgvector to an existing Postgres host):
   `sudo apt install postgresql-16-pgvector`. Verify with
   `SELECT extversion FROM pg_extension WHERE extname='vector'`.
3. **Source** (only if managed PG vendor lacks the package, e.g., legacy AWS
   RDS minor-version lag): `git clone --branch v0.8.2`, `make && make install`.
   Requires `postgresql-server-dev-16`. Avoid in production unless we own
   the host — vendor-managed PG (RDS, Aurora, Cloud SQL, Neon, Supabase) all
   ship pgvector ≥ 0.7 directly, see §1.5 of POSTGRES-A.

After install, the migration prelude (POSTGRES-B) runs
`CREATE EXTENSION IF NOT EXISTS vector;` exactly once per database.

---

## §2 Recommended HNSW config

We mirror upstream's parameters verbatim — they were chosen for 1536-dim
OpenAI embeddings, our default. Numbers are intentionally conservative:

| Param                | Value | Pgvector default | Rationale |
|----------------------|-------|------------------|-----------|
| `m`                  | 16    | 16               | Sweet spot for 768–1536-dim; matches upstream `search_repository.py:535`. |
| `ef_construction`    | 128   | 64               | 2× default for higher build-time recall; matches upstream `:536`. |
| `hnsw.ef_search`     | 50    | 40               | Floor matches upstream's `max(50, ...)` clamp at `:1660`. |
| Operator class       | `vector_cosine_ops` | n/a   | See §3. |
| Vector type          | `vector(1536)`      | n/a   | `halfvec(1536)` only above ~1M rows; see §2.2. |
| `maintenance_work_mem` (build) | `'2GB'` | `64MB` | Build is dramatically faster when graph fits in RAM. |

DDL (lives in `postgres-B-tables-010-embeddings.sql`):

```sql
CREATE TABLE mcp_embeddings_1536_default (
  id          TEXT PRIMARY KEY,
  parent_id   TEXT GENERATED ALWAYS AS (data->>'parent_id') STORED,
  entity_type TEXT GENERATED ALWAYS AS (data->>'entity_type') STORED,
  embedding   vector(1536) NOT NULL,
  data        JSONB NOT NULL,
  tsv         tsvector GENERATED ALWAYS AS (
                to_tsvector('english',
                  coalesce(data->>'name','') || ' ' ||
                  coalesce(data->>'description',''))
              ) STORED,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ
);

SET maintenance_work_mem = '2GB';        -- session-scoped during migration
CREATE INDEX mcp_embeddings_1536_default_hnsw
  ON mcp_embeddings_1536_default
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 128);
CREATE INDEX mcp_embeddings_1536_default_tsv_gin
  ON mcp_embeddings_1536_default USING gin (tsv);
CREATE INDEX mcp_embeddings_1536_default_entity_type
  ON mcp_embeddings_1536_default (entity_type);
RESET maintenance_work_mem;
```

### §2.1 Scale guidance

- **≤10K rows** (initial state): defaults above. Sub-millisecond ANN; whole
  index fits in shared buffers.
- **10K–100K rows**: keep defaults. Expect 1–5 ms p95 for top-10.
- **100K–1M rows**: still keep `m=16`. If recall@10 < 0.95 in benchmark,
  bump `ef_construction` to 200 (rebuild required) before touching `m`.
- **≥1M rows**: revisit. Either `m=32` + halfvec (§2.2), or migrate the
  table to `pgvectorscale` (StreamingDiskANN). Out of scope for Phase A.

### §2.2 IVFFlat vs HNSW — why HNSW

- **HNSW**: graph-based; supports incremental insert/update without rebuild;
  better recall/latency curve at every scale we care about; index size 2–3×
  IVFFlat, which is acceptable for ≤1M rows.
- **IVFFlat**: requires `CREATE INDEX` after data is loaded (clusters depend
  on data distribution); rebuild on significant churn; lower memory.

We pick HNSW because (a) upstream uses it (parity), (b) writes are
incremental (servers/agents register continuously), and (c) our scale stays
under the regime where IVFFlat's memory advantage matters.

### §2.3 `halfvec` vs `vector(N)`

`halfvec` (pgvector ≥ 0.7) stores float16, halving on-disk + RAM size
(6 KB → 3 KB per 1536-dim vector). Recall loss is empirically <0.5% on
OpenAI embeddings. **Decision rule**: stay on `vector(1536)` until either
(a) the embeddings table exceeds ~1 GB on disk, or (b) the HNSW index can
no longer fit in RAM at our planned host size. POSTGRES-B leaves a
commented `halfvec(1536)` alternate in the DDL.

---

## §3 Cosine distance + similarity normalization

The pgvector cosine operator `<=>` returns **distance** in `[0, 2]`:
`cos_dist = 1 - cos_sim`. Upstream stores raw cosine similarity from
DocumentDB and normalizes with `(cos_sim + 1) / 2`. Substituting
`cos_sim = 1 - cos_dist`:

```
normalized = (cos_sim + 1) / 2
           = ((1 - cos_dist) + 1) / 2
           = (2 - cos_dist) / 2
           = 1 - (cos_dist / 2)
```

So the SQL boundary applies `1.0 - (embedding <=> $1::vector) / 2.0` to
produce the same `[0, 1]` score that callers of upstream
`search_repository.py:1186-1187` already expect (1 = identical, 0 =
opposite, direction preserved).

**Rule of thumb**: never expose raw `cos_dist` past the repository layer.
Normalize at the SQL boundary so callers see the same `[0, 1]` score
shape as the DocumentDB path.

---

## §4 Hybrid search SQL

P3 §3.5 calls for vector candidates JOIN keyword `tsvector` boost. The
SQL below mirrors upstream's hybrid pipeline at
`search_repository.py:1170-1200`: vector ANN top-`k*3` (floor 50), text
rank multiplier `0.1`, path-match boost `5.0`, name-match boost `3.0`,
final score clamped `[0, 1]`.

```sql
WITH vector_candidates AS (
  SELECT id, data, parent_id,
         (embedding <=> $1::vector) AS cos_dist
  FROM   mcp_embeddings_1536_default
  WHERE  ($entity_type IS NULL OR entity_type = $entity_type)
  ORDER  BY embedding <=> $1::vector
  LIMIT  GREATEST($k * 3, 50)
),
scored AS (
  SELECT
    vc.id, vc.data, vc.parent_id,
    1.0 - (vc.cos_dist / 2.0)                           AS normalized_vector,
    CASE
      WHEN lower(vc.data->>'path') LIKE '%' || lower($2) || '%' THEN 5.0
      WHEN lower(vc.data->>'name') LIKE '%' || lower($2) || '%' THEN 3.0
      ELSE 0.0
    END                                                  AS text_boost
  FROM vector_candidates vc
)
SELECT id, data,
       LEAST(1.0,
             GREATEST(0.0,
                      normalized_vector + text_boost * 0.1)) AS relevance_score
FROM   scored
ORDER  BY relevance_score DESC
LIMIT  $k;
```

Notes:

- The `WHERE entity_type = ...` filter relies on the B-tree on
  `entity_type`; combined with iterative scans (§5) this gives correct
  top-k under selective filters.
- `tsv @@ plainto_tsquery(...)` can be added as an OR clause to expand
  the candidate pool when the query has rich keywords; we keep the
  default narrow because upstream does — preserves recall parity.
- All bind parameters are placeholders for asyncpg; no string
  interpolation.

---

## §5 Per-query tuning (`SET LOCAL hnsw.ef_search`)

`hnsw.ef_search` controls the dynamic candidate-list size at query time
(default 40). Larger = better recall, higher latency. Our pattern:

```sql
BEGIN;
SET LOCAL hnsw.ef_search = 50;            -- default for app queries
-- run hybrid SQL from §4
COMMIT;
```

`SET LOCAL` is required so the value resets at txn end and doesn't leak
across pooled connections (POSTGRES-D §2). The repository wraps every
ANN call in an implicit transaction and emits the SET as the first
statement. We expose two presets:

| Preset      | `ef_search` | Use case                              |
|-------------|-------------|---------------------------------------|
| `fast`      | 40          | UI typeahead; recall ≈ 0.92           |
| `default`   | 50          | App queries (matches upstream floor)  |
| `precise`   | 100         | Reranker pre-stage; recall ≈ 0.98     |
| `exhaustive`| 200         | Eval / debugging only                 |

For filtered queries that drop too many candidates after ANN, set
`SET LOCAL hnsw.iterative_scan = strict_order;` (pgvector 0.8+) so the
scan keeps pulling from the graph until top-k post-filter is reached.

---

## §6 Index maintenance

- **No manual VACUUM needed** for HNSW correctness. The index auto-updates
  on every INSERT and UPDATE (graph node added/relinked in-place); deleted
  rows are tombstoned and reclaimed by autovacuum on the heap.
- **VACUUM is slow on HNSW** because it walks every graph node looking
  for tombstones. Per pgvector docs, `REINDEX INDEX CONCURRENTLY` first
  *then* `VACUUM` is faster than VACUUM alone. We schedule weekly
  `REINDEX CONCURRENTLY` only when bloat % from `pg_stat_user_indexes` >
  20 %.
- **Autovacuum tuning** for the embeddings table: lower
  `autovacuum_vacuum_scale_factor` to `0.05` (default 0.2) so it runs
  more often on smaller deltas — large per-row width amplifies dead-tuple
  cost.
- **Build time** scales linearly with `ef_construction` and rows.
  Empirically: 100K × 1536-dim with `m=16, ef_construction=128` builds in
  ~30 s on a 4-core box with `maintenance_work_mem='2GB'`. Parallel
  builds (≥ 0.6) cut this further; set `max_parallel_maintenance_workers
  = 4` before the migration.

---

## §7 Backup considerations

- `pg_dump` writes the **CREATE INDEX DDL only**, not the index file
  contents. On `pg_restore`, the index is rebuilt by walking the table.
  Plan rebuild time per §6: ~30 s / 100K rows; ~5 min / 1M rows.
- During restore, run `pg_restore --jobs=4` with
  `maintenance_work_mem='2GB'` and
  `max_parallel_maintenance_workers=4` set on the target. We document
  these in the runbook produced by POSTGRES-D.
- **Physical backups** (`pg_basebackup`, EBS snapshots, WAL-E) preserve
  the index files as-is — preferred for fast PITR. Logical backups
  (`pg_dump`) are for cross-version moves only.
- **HA replication**: streaming replicas inherit the index without
  rebuild. Logical replication (subscriptions) ships row data only;
  the subscriber rebuilds the HNSW index just like a restore.
- Document the rebuild SLA: 1M-row table with HNSW must finish
  rebuild in <10 min on the migration host class. If we miss this,
  block the cutover and either (a) increase parallelism / RAM or
  (b) switch to physical-backup-based promotion.

---

## §8 Performance benchmarks

Numbers below are published by external benchmarks (Supabase, Neon,
Crunchy Data, Mastra, Instaclustr) at 1536 dim with HNSW
`m=16, ef_construction=64..128`. We verify on our data before launch,
not before — see "benchmarks lie" caveat in the New Stack piece.

| Scale | p50 (ms) | p95 (ms) | Notes |
|-------|----------|----------|-------|
| 10K   | <1       | 1–2      | Whole graph in shared buffers |
| 100K  | 1–2      | 3–5      | Single-node, default config |
| 1M    | 3–5      | 5–8      | Reported by Supabase/Mastra; ~5,250× faster than seq scan |
| 10M   | 8–15     | 20–40    | Requires graph in RAM; consider halfvec or pgvectorscale |

Throughput at single-instance: 5K–15K QPS for typical 1024–1536-dim HNSW
under standard mixed read workload. RAG end-to-end latency is dominated
by *embedding generation* (~50–100 ms), not the ANN query.

Storage: `vector(1536)` = 6 KB/row + ~50 % HNSW overhead at `m=16`. For
1M rows: ≈ 9 GB heap + index. `halfvec(1536)` halves both.

References (URLs cited in the search agent's report, not re-fetched here):

- pgvector README & releases — github.com/pgvector/pgvector
- Supabase HNSW vs IVFFlat benchmark
- Mastra "Benchmarking pgvector RAG performance"
- Markaicode "Production RAG System with pgvector"
- AWS database blog "IVFFlat and HNSW deep dive"
- Neon docs on `maintenance_work_mem` for HNSW builds
- The New Stack "Why pgvector benchmarks lie" — calibration warning

---

**Open items for Phase B**
1. Decide whether `pgvectorscale` is in-scope for the 100K → 1M
   transition or strictly Phase 3.
2. Wire `hnsw.iterative_scan = strict_order` default-on once we confirm
   no regression on unfiltered queries.
3. Add a benchmark fixture (10K, 100K rows of synthetic OpenAI-shaped
   vectors) under `tests/perf/` so we can re-run §8 numbers in CI.
