# Postgres backend — deploy guide

Runbook for `STORAGE_BACKEND=postgres` (opt-in; the Mongo / DocumentDB
stack is unaffected). Pairs with `docs/postgres-backend-design/`.

## 1. Required env

| Variable | Required | Notes |
|---|---|---|
| `STORAGE_BACKEND` | yes | Must be `postgres`. Default in the postgres-backend image. |
| `POSTGRES_DSN` | yes | e.g. `postgresql://mcp_registry:secret@db.internal:5432/mcp_registry`. Composed from the compose default if unset. |
| `POSTGRES_NAMESPACE` | yes | Multi-tenant table-name suffix. `[a-z][a-z0-9_]{0,30}`. Default `default`. |
| `POSTGRES_PASSWORD` | yes (compose) | Seeds the `pgvector/pgvector:pg16` container. Required even if `POSTGRES_DSN` is set. |
| `EMBEDDINGS_MODEL_DIMENSIONS` | yes | Drives the `mcp_embeddings_{N}` table. Must match the embeddings provider (384 / 1024 / 1536 / 3072). |
| `POSTGRES_SSL_MODE` | no | `disable` / `prefer` / `require` / `verify-ca` / `verify-full`. For RDS/Aurora set `verify-full` and provide `POSTGRES_SSL_CA_FILE`. |
| `POSTGRES_POOL_MIN` / `POSTGRES_POOL_MAX` | no | asyncpg pool bounds. Defaults 2 / 20. |
| `VECTOR_SEARCH_EF_SEARCH` | no | HNSW recall knob. Default 100. |

Full env matrix: `docs/postgres-backend-design/D-connection-mgmt.md`.

## 2. First-time deploy

```bash
# Set required env in .env
cat >> .env <<'EOF'
POSTGRES_PASSWORD=...
POSTGRES_NAMESPACE=default
EMBEDDINGS_MODEL_DIMENSIONS=384
SECRET_KEY=...
EOF

# 1. Bring up the database (profile-gated — does not affect mongo stack)
docker compose -f docker-compose.yml -f docker-compose.postgres.yml \
    --profile postgres up -d postgres

# 2. Run migrations (extensions, role, 14 tables, embeddings index)
docker compose -f docker-compose.yml -f docker-compose.postgres.yml \
    --profile postgres run --rm registry-postgres alembic upgrade head

# 3. Bring up the registry
docker compose -f docker-compose.yml -f docker-compose.postgres.yml \
    --profile postgres up -d registry-postgres
```

For RDS / Cloud SQL: skip step 1, set `POSTGRES_DSN` to the managed
instance, and run `alembic upgrade head` from any host that can reach it.

## 3. Healthcheck verification

```bash
docker inspect --format='{{.State.Health.Status}}' mcp-postgres
curl -s http://localhost:7860/health | jq '.storage'
# expected: {"status":"ok","backend":"postgres","version":"PostgreSQL 16...","namespace":"default",...}

docker exec -it mcp-postgres psql -U mcp_registry -d mcp_registry \
    -c "SELECT extname, extversion FROM pg_extension ORDER BY extname;"
# expected: pgcrypto, pg_stat_statements, plpgsql, vector  (pg_cron may be absent on managed PG)
```

If `/health` reports `"backend":"file"` or `"mongo"`, the container did not
pick up `STORAGE_BACKEND=postgres` — recheck `.env` and restart.

## 4. Backups

```bash
# Logical dump (custom format, compressed)
docker exec mcp-postgres pg_dump \
    -U mcp_registry -d mcp_registry --format=custom --compress=9 \
    > "mcp_registry-$(date -u +%Y%m%dT%H%M%SZ).pgdump"

# Restore into a fresh database
docker exec -i mcp-postgres pg_restore \
    -U mcp_registry -d mcp_registry --clean --if-exists \
    < mcp_registry-<TS>.pgdump
```

For RDS/Aurora rely on managed snapshots; take an ad-hoc dump before
major-version upgrades. For multi-database hosts use
`pg_dump --all-databases` from a libpq client outside the container.

## 5. Troubleshooting

- **`extension "vector" is not available`** — base image is wrong. The
  compose file pins `pgvector/pgvector:pg16`; do not swap it for plain
  `postgres:16`.
- **HNSW index never used** — run `EXPLAIN ANALYZE` on the vector-order
  query; if the plan shows Seq Scan, the table is below the planner's
  threshold. Diagnose with `SET LOCAL enable_seqscan = off`.
- **alembic role permission errors** — the prelude creates the
  `mcp_registry` role; run alembic as a superuser for the initial run,
  then revert the DSN to the app role.
- **`pg_cron unavailable`** — expected on Aurora / Cloud SQL.
  `backend_sessions` TTL falls back to the in-app sweeper documented in
  `migrations/postgres/postgres-B-tables-012-backend_sessions.sql`.
- **`pool exhausted` in logs** — raise `POSTGRES_POOL_MAX` and confirm
  `max_connections` on the database side.

Deeper diagnostics: `docs/postgres-backend-design/D-connection-mgmt.md`.
