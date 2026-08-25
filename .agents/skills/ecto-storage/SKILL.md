---
name: ecto-storage
description: Use when working with orchestrator persistence, storage adapters, repos, migrations, Ecto schemas, queries, SQL sandbox tests, transactions, SQLite/Postgres behavior, or persistence fixtures.
---

# Ecto Storage Skill

Use this skill for orchestrator persistence, storage adapters, repos,
migrations, Ecto schemas, Ecto queries, SQL sandbox tests, transactions,
SQLite/Postgres storage behavior, and persistence test fixtures.

## Core Rules

- Use the Tidewave MCP endpoint when the umbrella development server is running and the task involves storage-backed behavior, database inspection, runtime state, logs, source lookup, or persistence docs. One endpoint covers the whole node, orchestrator and storage included; see [`docs/contributing/dev-server.md`](../../../docs/contributing/dev-server.md).
- If Tidewave is unavailable because that server is not running, say so explicitly and continue with static inspection only when that is sufficient.
- Persistence stays behind orchestrator/storage behaviours.
- Do not leak Ecto repos or schemas into `favn_view`.
- Schema changes require migrations and tests.
- Multi-step persistence changes should use `Ecto.Multi` or explicit transactions where appropriate.
- SQLite/Postgres differences must be documented and tested when behavior differs.
- Storage adapter changes must preserve the public orchestrator contract.
- Use explicit data shapes and avoid hidden persistence coupling.
- Use Tidewave only in dev, and do not use runtime inspection to bypass Favn app boundaries.

## Tests

- Cover both supported storage adapters when applicable.
- Use SQL sandbox ownership correctly for repo-backed tests.
- Prefer shared fixtures and helpers from `apps/favn_test_support` when available.
- Before running PostgreSQL integration tests locally, ensure the repository's
  disposable PostgreSQL 18 container is running with `scripts/postgres/setup`.
  If port 5432 is already owned by another project, choose a free explicit port,
  for example `FAVN_POSTGRES_PORT=5433 scripts/postgres/setup`.
- Create a separate disposable `favn_test` database in that container and point
  both `FAVN_DATABASE_URL` and `FAVN_DATABASE_MIGRATOR_URL` at it with the local
  bootstrap role. Do not point integration tests at the normal `favn_dev`
  workspace: the test suite runs its own schema bootstrap and needs database
  `CREATE` authority that the hardened runtime and migrator roles intentionally
  lack. Also export the fixed test runtime-input pin key and version documented
  in `docs/storage/postgresql/testing.md`. Do not report PostgreSQL tests as
  unavailable until this setup has been attempted.
