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

Set a test URL for a disposable PostgreSQL 18 database:

```bash
export FAVN_DATABASE_URL=ecto://postgres:postgres@127.0.0.1:5432/favn_test
export FAVN_RUNTIME_INPUT_PIN_KEY=0123456789abcdef0123456789abcdef
export FAVN_RUNTIME_INPUT_PIN_KEY_VERSION=1
```

Apply migrations before integration tests:

```bash
MIX_ENV=test mix favn.postgres.migrate
```

Provision the local-development workspace when exercising the full dev stack:

```bash
MIX_ENV=test mix favn.postgres.provision_workspace \
  --id local-dev --slug local-dev --name "Local Development"
```

## Focused verification

From the umbrella root, prefer an app-scoped command:

```bash
MIX_ENV=test mix do --app favn_storage_postgres cmd mix test --no-compile
```

Use the exact owning test for regressions. For example, the split-root runtime
test is:

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile \
  --only slow --timeout 1200000 \
  test/integration/dev_split_root_regression_test.exs
```

The split-root test records runtime state plus runner and operator log tails if
the dev command exits before readiness. Preserve this diagnostic context: a
connection refusal describes only the failed health check, not the operator's
root startup error.

## CI topology

Each GitHub Actions test job receives a fresh PostgreSQL service and applies the
same migrations. No state is shared between fast, acceptance, and slow jobs.
PostgreSQL integration tests must not depend on developer machines, a persistent
cloud instance, or execution order from another job.

Production-like TLS, least-privilege roles, backup/restore, and multi-node failure
tests belong in explicit acceptance/slow slices because a basic service container
does not prove those properties.
