"""
alembic/env.py — runtime config + migration entrypoint.

Reads the connection DSN from $POSTGRES_DSN (or the libpq-style env vars
$PGHOST / $PGUSER / $PGPASSWORD / $PGDATABASE / $PGPORT as fallbacks) so the
same alembic config works inside the registry-postgres container, in CI, and
on operator laptops.

Migrations live in alembic/versions/. The initial revision (0001_initial.py)
loads the canonical SQL files in migrations/postgres/ in order — the SQL
files are the source of truth, alembic is just the runner + version
tracker.

See docs/postgres-backend-deploy.md for the operator runbook.
"""

from __future__ import annotations

import logging
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")


def _resolve_dsn() -> str:
    """
    Resolve the SQLAlchemy URL in priority order:
      1. $POSTGRES_DSN (preferred — single var, mirrors registry runtime)
      2. $DATABASE_URL (CI convention)
      3. libpq-style $PG* env vars
      4. sqlalchemy.url from alembic.ini (placeholder default)

    Always returns a `postgresql+psycopg://` URL so SQLAlchemy uses psycopg3
    (alembic doesn't speak asyncpg natively; psycopg is sync-only and only
    used during the migration window).
    """
    dsn = os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        host = os.environ.get("PGHOST")
        if host:
            user = os.environ.get("PGUSER", "mcp_registry")
            pwd = os.environ.get("PGPASSWORD", "")
            db = os.environ.get("PGDATABASE", "mcp_registry")
            port = os.environ.get("PGPORT", "5432")
            auth = f"{user}:{pwd}@" if pwd else f"{user}@"
            dsn = f"postgresql://{auth}{host}:{port}/{db}"

    if not dsn:
        return config.get_main_option("sqlalchemy.url")

    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    if dsn.startswith("postgresql://"):
        dsn = "postgresql+psycopg://" + dsn[len("postgresql://"):]

    return dsn


config.set_main_option("sqlalchemy.url", _resolve_dsn())

# No declarative metadata — migrations are hand-written SQL.
target_metadata = None


def run_migrations_offline() -> None:
    """Render SQL to stdout instead of executing — for review pipelines."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Apply migrations against a live database."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
