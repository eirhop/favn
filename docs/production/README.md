# Production Readiness

PostgreSQL 18 is Favn's only control-plane database. Storage V2 is implemented;
the remaining production work is release packaging, deployment proof, operator UI
completion, and data-plane recovery.

## Current status

| Area | Status | Production gate |
| --- | --- | --- |
| PostgreSQL Storage V2 | Implemented | Prove it against a production-sized restored snapshot and managed-provider PITR. |
| Workspace isolation, fencing, idempotency, bounded reads | Implemented | Record load, contention, failover, and recovery evidence. |
| Backend single-node launcher | Project-local prototype | Replace it with relocatable, supported release artifacts. |
| Web and orchestrator artifacts | Metadata only | Build runnable releases and define one supported topology. |
| Operator UI | Prototype | Finish core flows, audit mutations, and add browser acceptance. |
| DuckDB/DuckLake data plane | Prototype | Define and verify backup, recovery, cancellation, and failure behavior. |
| Multi-node control-plane coordination | Implemented foundation | Not an initial single-node release gate; package and prove it before claiming application-node failover. |

Favn can support a controlled backend pilot operated from the source/runtime
workspace. It is not yet a repeatable, supportable production distribution.

## Release gates

1. [Produce relocatable web, orchestrator, and runner releases](https://github.com/eirhop/favn/issues/522) with explicit
   configuration, secrets, health, upgrade, rollback, and compatibility contracts.
2. [Land durable asynchronous scheduling and submission](https://github.com/eirhop/favn/issues/525) with explicit throughput limits.
3. [Define and test data-plane durability](https://github.com/eirhop/favn/issues/526) independently of PostgreSQL control-plane
   recovery.
4. [Complete the production operator UI](https://github.com/eirhop/favn/issues/524), command audit trail, administrative session
   controls, and browser acceptance path.
5. [Prove PostgreSQL operation on the target managed service](https://github.com/eirhop/favn/issues/523), including restore,
   load, failover, observability, and tenant-isolation evidence.

Build #523's telemetry and drill tooling early, then use it to qualify the final
single-node release. Multi-node application failover is a later topology claim,
not a gate for the first supported single-node target.

## Canonical documents

- [`control_plane_environment.md`](control_plane_environment.md) defines the
  implemented same-BEAM environment, proxy, HTTP, and secret contract.
- [`../storage/postgresql/architecture.md`](../storage/postgresql/architecture.md)
  defines the implemented storage architecture.
- [`../storage/postgresql/data-model.md`](../storage/postgresql/data-model.md)
  documents the schema.
- [`postgresql_operator_runbook.md`](postgresql_operator_runbook.md) defines
  PostgreSQL operations.
- [`single_node_contract.md`](single_node_contract.md) defines the current
  project-local backend launcher.
- [`../FEATURES.md`](../FEATURES.md) lists shipped capability and current limits.
- [`../ROADMAP.md`](../ROADMAP.md) lists only the remaining production work.

PostgreSQL recovery covers the Favn control plane. It does not back up DuckDB
files, DuckLake metadata, object storage, warehouses, customer source systems, or
external secret stores.
