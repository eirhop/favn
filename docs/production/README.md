# Production Readiness

PostgreSQL 18 is Favn's only control-plane database. Storage V2, immutable
control-plane and customer-owned runner releases, Docker-free source
development, and a direct image compatibility gate are
implemented. Remaining product work includes full deployment drills, operator
UI completion, managed-provider evidence, and data-plane recovery.

## Current status

| Area | Status | Production gate |
| --- | --- | --- |
| PostgreSQL Storage V2 | Implemented | Prove it against a production-sized restored snapshot and managed-provider PITR. |
| Azure PostgreSQL managed identity | Connection/reconnect and release operations implemented; infrastructure remains operator-owned | Run #588's passwordless, long-duration reconnect acceptance in the target Azure environment. |
| Workspace isolation, fencing, idempotency, bounded reads | Implemented | Record load, contention, failover, and recovery evidence. |
| Lifecycle, readiness, bounded drain | Implemented | Owning-layer tests cover policy; real signal, cancellation, and recovery drills remain target-environment evidence. |
| Control-plane release image | Implemented | CI directly builds, scans, attests, and publishes green commit images. |
| Customer runner and manifest releases | Implemented boundary | Customer CI and target-environment execution and upgrade/rollback drills remain. |
| Operator UI | Security/admin baseline and HTTP browser smoke gate implemented | Complete #579's role/workspace/Entra matrix plus manual keyboard, screen-reader, visual, and target-Azure qualification. |
| Security qualification | HTTP-boundary phase implemented | Complete #578's runner, PostgreSQL, abuse-pressure, artifact/secret, and final release-gate phases. |
| DuckDB/DuckLake data plane | Prototype | Define and verify backup, recovery, cancellation, and failure behavior. |
| Multi-node control-plane coordination | Implemented foundation | Deferred; package and prove it before claiming application-node failover. |

Favn has runnable OCI artifacts, documented upgrade and rotation procedures,
and a direct production-image compatibility gate. Operators still own
the platform network, firewall, reverse proxy, database service, customer
runner image, and deployment-specific qualification.

Local development is a separate Docker-free workflow. Developers supply
PostgreSQL and environment variables; Favn starts local BEAM processes and
never manages containers or durable database data.

## Release gates

1. [Ship the portable control-plane and customer-runner release](https://github.com/eirhop/favn/issues/522) with explicit
   configuration, secrets, health, upgrade, rollback, and compatibility contracts.
2. [Land durable asynchronous scheduling and submission](https://github.com/eirhop/favn/issues/525) with explicit throughput limits.
3. [Define and test data-plane durability](https://github.com/eirhop/favn/issues/526) independently of PostgreSQL control-plane
   recovery.
4. [Complete the production operator UI](https://github.com/eirhop/favn/issues/524), command audit trail, administrative session
   controls, and browser acceptance path.
5. [Prove PostgreSQL operation on the target managed service](https://github.com/eirhop/favn/issues/523), including restore,
   load, failover, observability, and tenant-isolation evidence.
6. [Qualify the production attack surface](https://github.com/eirhop/favn/issues/578) on every release candidate and attach
   the separate Azure/Entra and manual accessibility evidence.

Build #523's telemetry and drill tooling early, then use it to qualify the first
supported one-control-plane/elastic-runner deployment on the target managed
service. Multi-control-plane application failover is a later topology claim.

## Canonical documents

- [`elastic_runners.md`](elastic_runners.md) defines arbitrary runner pools,
  durable demand, elastic self-exit, mutual-TLS distribution, and
  infrastructure-neutral scaling.
- [`control_plane_environment.md`](control_plane_environment.md) defines the
  implemented same-BEAM environment, proxy, HTTP, lifecycle, shutdown, and secret
  contract.
- [`deployment_topology.md`](deployment_topology.md) defines artifact ownership,
  operator-owned infrastructure, startup order, and runtime limitations.
- [`control_plane_image.md`](control_plane_image.md) defines the immutable OCI
  images, direct repository build, publishing, and maintainer candidate path.
- [`runner_releases.md`](runner_releases.md) defines runner/manifest identities,
  rebuild classification, customer image ownership, publication, and activation.
- [`upgrade_and_rollback.md`](upgrade_and_rollback.md) defines control-plane,
  runner-plus-manifest, and manifest-only procedures.
- [`network_and_proxy.md`](network_and_proxy.md) defines the trusted network,
  private ports, distributed-Erlang limitations, and reverse-proxy contract.
- [`secret_rotation.md`](secret_rotation.md) defines environment-only manual
  overlap, restart, inventory, and invalidation procedures.
- [`operator_security.md`](operator_security.md) defines operator roles,
  workspace/session behavior, administration, audit, qualification, and known
  security limits.
- [`security_qualification.md`](security_qualification.md) defines the
  HTTP-boundary threat model, complete route catalogue, isolated attacker
  topology, evidence rules, phase verdict, and explicit remaining boundaries.
- [`local_docker_compose.md`](local_docker_compose.md) defines the optional
  customer-owned single-host deployment example.
- [`issue_522_acceptance_matrix.md`](issue_522_acceptance_matrix.md) maps the
  portable production contract to executable container evidence.
- [`issue_523_acceptance_matrix.md`](issue_523_acceptance_matrix.md) defines the
  local PostgreSQL load/recovery qualification and the separately deferred
  managed-service acceptance work.
- [`../storage/postgresql/architecture.md`](../storage/postgresql/architecture.md)
  defines the implemented storage architecture.
- [`../storage/postgresql/data-model.md`](../storage/postgresql/data-model.md)
  documents the schema.
- [`postgresql_operator_runbook.md`](postgresql_operator_runbook.md) defines
  PostgreSQL operations.
- [`../FEATURES.md`](../FEATURES.md) lists shipped capability and current limits.
- [`../ROADMAP.md`](../ROADMAP.md) lists only the remaining production work.

PostgreSQL recovery covers the Favn control plane. It does not back up DuckDB
files, DuckLake metadata, object storage, warehouses, customer source systems, or
external secret stores.
