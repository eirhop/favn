# Production Readiness

PostgreSQL 18 is Favn's only control-plane database. Storage V2, immutable
control-plane and customer-owned runner releases, Docker-first local
development, and a representative container compatibility gate are
implemented. Remaining product work includes full deployment drills, operator
UI completion, managed-provider evidence, and data-plane recovery.

## Current status

| Area | Status | Production gate |
| --- | --- | --- |
| PostgreSQL Storage V2 | Implemented | Prove it against a production-sized restored snapshot and managed-provider PITR. |
| Workspace isolation, fencing, idempotency, bounded reads | Implemented | Record load, contention, failover, and recovery evidence. |
| Lifecycle, readiness, bounded drain | Implemented | Owning-layer tests cover policy; real signal, cancellation, and recovery drills remain target-environment evidence. |
| Control-plane release image | Implemented | A manual workflow publishes an exact current, green `main` revision to GHCR. Ordinary merges publish nothing. |
| Customer runner and manifest releases | Implemented boundary | The representative container gate covers automatic customer build, release-ID alignment, health, and DuckDB ADBC loading; execution and upgrade/rollback drills remain. |
| Operator UI | Prototype | Finish core flows, audit mutations, and add browser acceptance. |
| DuckDB/DuckLake data plane | Prototype | Define and verify backup, recovery, cancellation, and failure behavior. |
| Multi-node control-plane coordination | Implemented foundation | Deferred; package and prove it before claiming application-node failover. |

Favn has deterministic control-plane inputs, runnable OCI artifacts,
Docker-first installation, documented upgrade and rotation procedures, and a
representative production-container compatibility gate. Operators still own
the platform network, firewall, reverse proxy, database service, customer
runner image, and deployment-specific qualification.

Local development now uses the same ownership boundary: install selects only
the immutable control-plane image, while the customer commits and may extend a
versioned, labeled Compose template. Favn-generated credentials and image
selections stay under `.favn/`; stop and reset never delete consumer Compose
resources or durable data.

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

Build #523's telemetry and drill tooling early, then use it to qualify the first
supported one-control-plane/one-runner deployment on the target managed service.
Multi-node application failover is a later topology claim.

## Canonical documents

- [`control_plane_environment.md`](control_plane_environment.md) defines the
  implemented same-BEAM environment, proxy, HTTP, lifecycle, shutdown, and secret
  contract.
- [`deployment_topology.md`](deployment_topology.md) defines artifact ownership,
  operator-owned infrastructure, startup order, and runtime limitations.
- [`control_plane_image.md`](control_plane_image.md) defines the immutable OCI
  image, GHCR publishing, selective build identity, and maintainer candidate path.
- [`runner_releases.md`](runner_releases.md) defines runner/manifest identities,
  rebuild classification, customer image ownership, publication, and activation.
- [`upgrade_and_rollback.md`](upgrade_and_rollback.md) defines control-plane,
  runner-plus-manifest, and manifest-only procedures.
- [`network_and_proxy.md`](network_and_proxy.md) defines the trusted network,
  private ports, distributed-Erlang limitations, and reverse-proxy contract.
- [`secret_rotation.md`](secret_rotation.md) defines environment-only manual
  overlap, restart, inventory, and invalidation procedures.
- [`local_docker_compose.md`](local_docker_compose.md) defines prerequisites,
  private GHCR access, Compose parity, reload classification, and cleanup.
- [`issue_522_acceptance_matrix.md`](issue_522_acceptance_matrix.md) maps the
  portable production contract to executable container evidence.
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
