# Project-Local PostgreSQL Backend Launcher Contract

This document defines the project-local one-node backend launcher used for
development, acceptance, and controlled internal pilots. It uses PostgreSQL
Storage V2 and is not an embedded-database mode. It is not yet the relocatable,
supported production distribution tracked by issue #522.

Current persistence behavior lives in `docs/storage/postgresql/`; the accepted
PostgreSQL-only decision and invariants remain in
`docs/architecture/postgresql-control-plane-storage-v2.md`.

## Topology

One BEAM node contains:

- the orchestrator API and scheduler;
- one local runner;
- the PostgreSQL persistence client and background publishers/projectors.

PostgreSQL is external and may be shared with other Favn nodes. Customer analytics
data remains outside the control plane in each workspace's dedicated blob and
DuckLake infrastructure.

The launcher does not include the Phoenix UI. The current UI implementation calls
the orchestrator through an in-BEAM facade; there is no supported split web release
or network boundary yet.

This topology has no application-node failover. A node outage stops dispatch until
the node restarts, although committed control-plane state remains durable. The
supported multi-node application topology is deferred to issue #522; operators
must not infer it from the database coordination contracts alone.

## Artifact

`mix favn.build.single` produces a verified project-local backend launcher. It is
not a relocatable OTP release and depends on the recorded build/runtime source
root. Its `dist_dir` is immutable after build; PID files, logs, and local DuckDB
data go under `FAVN_SINGLE_NODE_HOME`.

The launcher must:

- accept only PostgreSQL storage;
- validate production runtime configuration before opening the API;
- fail readiness until the exact `favn_control` schema is compatible;
- never run migrations or create a workspace at startup;
- require explicit workspace identity for bootstrap and every customer operation;
- preserve committed manifests, runs, events, identity, and projections across
  process restart;
- keep the service token and operator session boundaries distinct.

## Database prerequisites

Before startup, an operator must:

1. migrate with the separate migrator identity;
2. grant the runtime role;
3. provision every allowed workspace explicitly;
4. configure verified TLS and the runtime-input encryption key;
5. configure operator bootstrap credentials and service identities.

Production always uses `verify-full` TLS with an absolute CA file. CI and local
acceptance may connect to a plaintext loopback PostgreSQL only by setting both
`FAVN_DATABASE_SSL_MODE=disable` and
`FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE=true`. The unsafe interlock must never be
present in a deployed environment.

## Bootstrap

`mix favn.bootstrap.single` publishes the immutable manifest release, activates an
exact deployment for `--workspace-id`, registers the local runner, and verifies the
active manifest through authenticated APIs. Repeating the command with the same
manifest is idempotent.

Operator credentials are required even with `--no-activate`, because runner
registration is workspace-authorized.

## Acceptance

The executable contract is
`apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs` and is
summarized in `docs/production/single_node_acceptance_matrix.md`.

The PostgreSQL operator procedures for TLS, roles, migrations, backups, restores,
retention, and incidents are in
`docs/production/postgresql_operator_runbook.md`.
