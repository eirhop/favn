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

Use the exact owning test for regressions. The production-like PostgreSQL,
control-plane, and runner topology is covered by the container acceptance:

```bash
FAVN_CONTROL_PLANE_CANDIDATE=favn-control-plane-candidate:<build-id> \
  mix test.container
```

Failed Compose startup preserves bounded, redacted diagnostics under
`.favn/logs/compose-failure.log`. Use `mix favn.logs --service control-plane`
and `mix favn.logs --service runner` for the current service logs; a connection
refusal alone does not identify the first startup failure.

## CI topology

Each GitHub Actions test job receives a fresh PostgreSQL service and applies the
same migrations. No state is shared between fast, acceptance, and slow jobs.
PostgreSQL integration tests must not depend on developer machines, a persistent
cloud instance, or execution order from another job.

Production-like TLS, least-privilege roles, backup/restore, and multi-node failure
tests belong in explicit acceptance/slow slices because a basic service container
does not prove those properties.
