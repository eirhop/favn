# Production deployment topology

The supported platform-neutral topology has:

1. one external PostgreSQL 18 database;
2. one always-on Favn Orchestrator container, fixed at one replica;
3. zero or one independently scalable Favn View container; and
4. zero to many customer-built runner processes, grouped by arbitrary logical
   pool and immutable release.

The control plane can be small because runners execute customer and data-plane
work. Elastic runners may scale to zero while idle. Resident runners are
appropriate only for continuous work or when cold-start latency is
unacceptable.

Favn does not provision a virtual network, container service, Kubernetes
cluster, VM, runner Job, database, registry, or scaler. The operator deploys
immutable OCI digests and maps the provider-neutral demand contract to their
infrastructure.

View and Orchestrator use clustered Phoenix PubSub for live wake-ups. A cold
View rereads PostgreSQL-backed Orchestrator state; PubSub is never the durable
truth. Scaling View to zero therefore stops only browser delivery, not schedules,
runs, recovery, or persisted operator state.

## Artifact ownership

| Artifact | Owner | Deployment identity |
| --- | --- | --- |
| Control-plane roles image | Favn | One immutable OCI digest selected with `FAVN_CONTROL_PLANE_ROLE` |
| Runner image | Customer | Immutable OCI digest plus baked runner release |
| Manifest archive and version | Customer project | Archive SHA-256, manifest ID, content hash, and pool-to-release map |
| PostgreSQL database | Operator | PostgreSQL 18 service and exact Favn schema |
| Scaler/Job definition | Operator | Pool and release partition |

Favn publishes no customer runner image. A project may build different images
for `duckdb`, `pure_elixir`, `gpu`, or any other user-defined pool. Each
manifest maps the pools it uses to exact runner releases.

Manifest transport does not require another image build. The customer build
produces one deterministic `.tar.gz`; an authorized client uploads it unchanged
to the private Orchestrator API. CI can do this directly when it has network
access. Otherwise blob storage plus a small private relay Job is one possible
infrastructure choice. Blob storage and that Job are not required Favn
components, and the Orchestrator needs no mounted artifact volume. The canonical
user workflow is [manifest deployment](../../apps/favn/guides/manifest-deployment.md).

The copied Compose file is a resident single-host example. It exercises the
same registration and durable claim protocol, but it is not an autoscaling
platform. Container Apps Jobs, Kubernetes/KEDA, ECS, Nomad, and VM supervisors
implement the same demand/start/self-exit contract.

## Required infrastructure

Provide:

- a trusted private network for View, Orchestrator, runners, and PostgreSQL;
- stable private DNS names and fixed distribution ports for View and Orchestrator;
- mutual-TLS distributed BEAM and a high-entropy cookie;
- View and runner outbound access to the Orchestrator EPMD/distribution listeners;
- an HTTPS reverse proxy or VPN for operators;
- no public route to PostgreSQL, the private API, EPMD, or BEAM distribution;
- separate PostgreSQL migrator and least-privilege runtime identities;
- secret injection and certificate rotation; and
- termination grace longer than Favn's configured drain budget.

## Deployment order

1. Select the control-plane image by immutable digest.
2. Build each runner image with its immutable release ID.
3. Build the manifest archive with the exact pool-to-release map and retain its
   printed SHA-256.
4. Back up PostgreSQL and run the candidate image's one-off `upgrade` Job. For a
   new environment, authorize and run the one-off `bootstrap` Job instead, then
   remove its temporary administrator identity. Both commands verify through
   the runtime identity; runtime startup never migrates.
5. Start exactly one Orchestrator and verify readiness. Zero runners and zero
   View replicas are valid.
6. Start the View at `0..1` replicas and verify that it reaches the Orchestrator
   through the public facade over mutual-TLS distributed Erlang.
7. Deploy one scaler/Job or resident definition for each pool/release
   partition.
8. Upload the archive through the private deployment endpoint and wait for the
   durable operation to finish. Keep its operation ID for status and safe replay.
   Acceptance and activation do not require a live runner; existing persisted
   SQL targets may finish with `needs_attention` when physical inspection is
   unavailable.
9. Submit a smoke run and verify demand, runner registration, claim, result,
   downstream DAG progress, idle self-exit, logs, and operator visibility.

During a runner-image or manifest rollback, restore the matching prior
pool-to-release map and image definition. Keep old and new partitions available
until the exact old partition reports durably drained.

See [PostgreSQL bootstrap](postgresql_bootstrap.md),
[elastic runners](elastic_runners.md),
[runner releases](runner_releases.md), and
[network contract](network_and_proxy.md).
