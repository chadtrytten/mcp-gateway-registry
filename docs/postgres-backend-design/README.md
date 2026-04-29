# Postgres+JSONB+pgvector backend — design memos (Phase Α)

CCLI2 batch-NEXT-27 Phase Α outputs from 6 fan-out opus agents (2026-04-29).
Together: ~70 KB design + ~37 KB Python skeletons + ~895 LoC SQL DDL + ~17 KB tests.

| Memo | Author | Subject |
|---|---|---|
| A | POSTGRES-A | Upstream codebase deep-read; abstraction-layer contract (14 ABCs catalogued; factory wiring; settings additions; Mongo-isms) |
| B | POSTGRES-B | Schema DDL + migration prelude (14 tables, see ../../migrations/postgres/) |
| C | POSTGRES-C | pgvector configuration + HNSW tuning |
| D | POSTGRES-D | asyncpg connection management + retry + health (skeleton at ../../registry/repositories/postgres/client.py) |
| E | POSTGRES-E | Mongo→SQL filter translator (8-op subset; impl at ../../registry/repositories/postgres/mongo_filter.py) |
| F | POSTGRES-F | RegistryCardRepository proof-of-concept (impl at ../../registry/repositories/postgres/registry_card_repository.py + tests at ../../tests/postgres/) |

See also: GitHub Discussion proposing this backend at
https://github.com/agentic-community/mcp-gateway-registry/discussions/904
