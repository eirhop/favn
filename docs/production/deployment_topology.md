# Production deployment topology

The supported platform-neutral topology has:

1. one external PostgreSQL 18 database;
2. one always-on Favn control-plane container running View and Orchestrator in
   the same BEAM; and
3. zero to many customer-built runner processes, grouped by arbitrary logical
   pool and immutable release.

The control plane can be small because runners execute customer and data-plane
work. Elastic runners may scale to zero while idle. Resident runners are
appropriate only for continuous work or when cold-start latency is
unacceptable.

Favn does not provision a virtual network, container service, Kubernetes
cluster, VM, runner Job, database, registry, or scaler. The operator deploys
immutable OCI digests and maps the provider-neutral demand contract to their
infrastructure.

## Artifact ownership

| Artifact | Owner | Deployment identity |
| --- | --- | --- |
| Control-plane image | Favn | Immutable OCI digest |
| Runner image | Customer | Immutable OCI digest plus baked runner release |
| Manifest version | Customer project | Manifest ID, content hash, and pool-to-release map |
| PostgreSQL database | Operator | PostgreSQL 18 service and exact Favn schema |
| Scaler/Job definition | Operator | Pool and release partition |

Favn publishes no customer runner image. A project may build different images
for `duckdb`, `pure_elixir`, `gpu`, or any other user-defined pool. Each
manifest maps the pools it uses to exact runner releases.

The copied Compose file is a resident single-host example. It exercises the
same registration and durable claim protocol, but it is not an autoscaling
platform. Container Apps Jobs, Kubernetes/KEDA, ECS, Nomad, and VM supervisors
implement the same demand/start/self-exit contract.

## Required infrastructure

Provide:

- a trusted private network for the control plane, runners, and PostgreSQL;
- a stable private DNS name and fixed distribution port for the control plane;
- mutual-TLS distributed BEAM and a high-entropy cookie;
- runner outbound access to the control-plane EPMD/distribution listeners;
- an HTTPS reverse proxy or VPN for operators;
- no public route to PostgreSQL, the private API, EPMD, or BEAM distribution;
- separate PostgreSQL migrator and least-privilege runtime identities;
- secret injection and certificate rotation; and
- termination grace longer than Favn's configured drain budget.

## Deployment order

1. Select the control-plane image by immutable digest.
2. Build each runner image with its immutable release ID.
3. Build and publish the manifest with the exact pool-to-release map.
4. Back up PostgreSQL, migrate with the candidate image, grant the runtime
   role, and verify the exact schema. Runtime startup never migrates.
5. Start the control plane and verify readiness. Zero runners is valid.
6. Deploy one scaler/Job or resident definition for each pool/release
   partition.
7. Activate the manifest. Activation does not require a live runner.
8. Submit a smoke run and verify demand, runner registration, claim, result,
   downstream DAG progress, idle self-exit, logs, and operator visibility.

During a runner-image or manifest rollback, restore the matching prior
pool-to-release map and image definition. Keep old and new partitions available
until the exact old partition reports durably drained.

See [elastic runners](elastic_runners.md),
[runner releases](runner_releases.md), and
[network contract](network_and_proxy.md).
