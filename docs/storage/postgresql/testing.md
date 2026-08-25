# Testing PostgreSQL Storage

Favn uses real PostgreSQL 18 for persistence integration tests. A cloud database
is not required for CI: GitHub Actions starts an ephemeral PostgreSQL service
container for each test job.

## Test layers

| Layer | Purpose | Database |
| --- | --- | --- |
| Orchestrator unit tests | Domain decisions, codecs, validation, and boundary contracts | None; test runtime is explicitly disabled. |
| Store integration tests | Transactions, constraints, idempotency, tenancy, and query results | Local/CI PostgreSQL 18. |
| Concurrency tests | Locks, fencing, claims, and competing writers | PostgreSQL with independent connections/processes. |
| Plan/performance contracts | Index usage, bounded query counts, and large payload behavior | Seeded PostgreSQL at representative cardinality. |
| Acceptance/slow tests | Migration, runtime composition, split-root dev, restore, and operational paths | Ephemeral PostgreSQL 18. |

## Clean-build requirement

All umbrella apps share `_build`. An app may also be compiled as a path
dependency by a nested Mix command. OTP application specifications must therefore
be environment-independent; test behavior belongs in application configuration,
not conditional `.app` metadata.

The test config sets `:favn_orchestrator, start_runtime: false`. This prevents
ordinary unit-test slices from booting a concrete database backend. Integration
and local-stack tests explicitly compose PostgreSQL and start the runtime they
exercise.

This rule prevents local stale build artifacts from hiding dependency or startup
errors that appear in a clean CI checkout.

## Local setup

Start the repository's PostgreSQL 18 container. If port 5432 is already used by
another project, choose a free port explicitly:

```bash
FAVN_POSTGRES_PORT=5433 scripts/postgres/setup
```

Create a separate disposable `favn_test` database in that container. Its owner
must be the local bootstrap role because the test suite creates and migrates its
own schema; the hardened `favn_runtime` and `favn_migrator` roles for `favn_dev`
intentionally lack that authority. Do not run the integration suite against the
normal local-development database.

```bash
docker compose -f compose.postgres.yml exec -T postgres \
  createdb --username favn_bootstrap --owner favn_bootstrap favn_test
```

Run that creation command only when `favn_test` does not already exist.

Set the test URL and use the selected port:

```bash
export FAVN_DATABASE_URL=ecto://favn_bootstrap:favn_bootstrap_local@127.0.0.1:5433/favn_test
export FAVN_DATABASE_MIGRATOR_URL=$FAVN_DATABASE_URL
export FAVN_RUNTIME_INPUT_PIN_KEY=0123456789abcdef0123456789abcdef
export FAVN_RUNTIME_INPUT_PIN_KEY_VERSION=1
```

Apply migrations before integration tests:

```bash
MIX_ENV=test mix favn.postgres.test_setup
```

This fixture task exists only in test builds. Production commands intentionally
reject the disposable PostgreSQL administrator used by the integration suite.

Provision the local-development workspace when exercising the full dev stack:

```bash
MIX_ENV=test mix favn.postgres.provision_workspace \
  --config .favn/workspace-bootstrap.json
```

## Focused verification

From the umbrella root, prefer an app-scoped command:

```bash
MIX_ENV=test mix do --app favn_storage_postgres cmd mix test --no-compile
```

Use the exact owning test for regressions. The control-plane image contract is
qualified separately from PostgreSQL integration:

```bash
docker build -f rel/control_plane/Dockerfile -t favn-control-plane:test .
scripts/control_plane_image_contract.sh favn-control-plane:test
```

For source development, inspect the `mix favn.dev` terminal and
`mix favn.diagnostics`. Deployment logs belong to the chosen platform.

## CI topology

Each GitHub Actions test job receives a fresh PostgreSQL service and applies the
same migrations. No state is shared between fast, acceptance, and slow jobs.
PostgreSQL integration tests must not depend on developer machines, a persistent
cloud instance, or execution order from another job.

Production-like TLS, least-privilege roles, backup/restore, and multi-node failure
tests belong in explicit acceptance/slow slices because a basic service container
does not prove those properties.
