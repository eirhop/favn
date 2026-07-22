# Production Readiness

PostgreSQL 18 is Favn's only control-plane database. Storage V2, immutable
control-plane and runner releases, Docker-first local development, and the
portable container acceptance contract are implemented. Remaining product work
includes operator UI completion, managed-provider evidence, and data-plane
recovery.

## Current status

| Area | Status | Production gate |
| --- | --- | --- |
| PostgreSQL Storage V2 | Implemented | Prove it against a production-sized restored snapshot and managed-provider PITR. |
| Workspace isolation, fencing, idempotency, bounded reads | Implemented | Record load, contention, failover, and recovery evidence. |
| Lifecycle, readiness, bounded drain | Implemented | Container acceptance covers idle/active SIGTERM, bounded cancellation, and recovery. |
| Control-plane release image | Implemented | Main CI publishes a changed verified build to GHCR after merge. |
| Customer runner and manifest releases | Implemented | Container acceptance covers execution, alignment, restart, upgrade, and rollback. |
| Operator UI | Prototype | Finish core flows, audit mutations, and add browser acceptance. |
| DuckDB/DuckLake data plane | Prototype | Define and verify backup, recovery, cancellation, and failure behavior. |
| Multi-node control-plane coordination | Implemented foundation | Deferred; package and prove it before claiming application-node failover. |

Favn has deterministic release inputs, runnable OCI artifacts, Docker-first
installation, and executable upgrade, rotation, and production-container
qualification. Operators still own the platform network, firewall, reverse
proxy, database service, and immutable deployment configuration.

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
- [`control_plane_image.md`](control_plane_image.md) defines the immutable OCI
  image, GHCR publishing, selective build identity, and maintainer candidate path.
- [`issue_522_acceptance_matrix.md`](issue_522_acceptance_matrix.md) maps the
  portable production contract to executable container evidence.
- [`../storage/postgresql/architecture.md`](../storage/postgresql/architecture.md)
  defines the implemented storage architecture.
- [`../storage/postgresql/data-model.md`](../storage/postgresql/data-model.md)
  documents the schema.
- [`postgresql_operator_runbook.md`](postgresql_operator_runbook.md) defines
  PostgreSQL operations.
- [`control_plane_environment.md`](control_plane_environment.md) defines the
  runtime configuration and secret contract.
- [`../FEATURES.md`](../FEATURES.md) lists shipped capability and current limits.
- [`../ROADMAP.md`](../ROADMAP.md) lists only the remaining production work.

PostgreSQL recovery covers the Favn control plane. It does not back up DuckDB
files, DuckLake metadata, object storage, warehouses, customer source systems, or
external secret stores.
