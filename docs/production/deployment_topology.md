# Production deployment topology

The first supported Favn deployment has exactly three durable/runtime roles:

1. one external PostgreSQL 18 database;
2. one Favn-published control-plane container running View and Orchestrator in
   the same BEAM; and
3. one customer-built runner container running the customer's Elixir code and
   runner plugins in a separate BEAM.

This is a platform-neutral application contract. Favn does not provision a
virtual network, firewall, VPN, reverse proxy, load balancer, container service,
database service, or customer registry. The operator supplies those resources
and deploys immutable OCI digests.

## Artifact ownership

| Artifact | Owner | Deployment identity |
| --- | --- | --- |
| Control-plane image | Favn | `ghcr.io/eirhop/favn-control-plane@sha256:<digest>` |
| Runner image | Customer | Customer registry image selected by immutable digest |
| Manifest release | Customer project | `manifest_version_id` plus `required_runner_release_id` |
| PostgreSQL database | Operator | PostgreSQL 18 service and Favn schema version |

Favn publishes no runner image. `mix favn.build.runner` creates a relocatable OCI
build context and an aligned manifest release; the customer builds and pushes
that context with their own registry credentials.

## Required infrastructure

Before deployment, provide:

- a trusted private network segment containing only the control plane, runner,
  and PostgreSQL service;
- private DNS names that match the configured long BEAM node names;
- firewall rules allowing PostgreSQL only from the control plane and allowing
  EPMD plus the fixed BEAM distribution port only between the two BEAM nodes;
- an HTTPS reverse proxy or VPN entry point for operators;
- no public route to PostgreSQL, EPMD, BEAM distribution, the runner, or the
  private Orchestrator API;
- a PostgreSQL migrator identity separate from the runtime identity;
- environment-variable injection for every deployment value and secret; and
- a termination grace period longer than Favn's configured drain budget.

A private IP address by itself is not a sufficient security boundary. The v1
runner transport is unencrypted distributed Erlang and grants the connected
runner node-level trust. Use it only inside the isolated trust zone described in
[`network_and_proxy.md`](network_and_proxy.md).

## Deployment order

1. Select the control-plane image by immutable digest.
2. Build the aligned customer runner and manifest release as described in
   [`runner_releases.md`](runner_releases.md), then select the runner image by
   immutable digest.
3. Back up PostgreSQL and run the candidate control-plane image's release-safe
   `preflight-upgrade`, `migrate`, `grant-runtime`, `verify-schema`, and
   workspace-provisioning operations as applicable. Runtime startup never
   migrates automatically.
4. Start the runner with its private node name, fixed distribution port, EPMD
   port, shared distribution cookie, and bounded shutdown configuration.
5. Start the control plane with the same network identity values and the
   complete environment contract from
   [`control_plane_environment.md`](control_plane_environment.md).
6. Require liveness and readiness before routing operator traffic. Readiness
   proves PostgreSQL schema/grants, lifecycle admission, scheduler health,
   runner connectivity, the self-verified runner release, and every active
   manifest's exact runner alignment.
7. Publish a manifest release, then activate its exact version for the intended
   workspace. Publication may occur while the runner is unavailable; activation
   cannot.
8. Execute one SQL asset and one Elixir asset as the deployment smoke test.

## Runtime and shutdown

The reverse proxy routes the public View listener only. The private Orchestrator
API is for trusted operator tooling on the private network. Container port
metadata does not publish a port; the platform's network rules remain
authoritative.

On termination, readiness becomes false before new work is rejected. The node
drains already admitted work for the configured bounded interval, uses ordinary
durable cancellation/result paths at the deadline, and preserves unknown
outcomes rather than inventing success. Do not send traffic again until the
replacement reports full readiness.

Upgrade and rollback are scheduled, drain-first operations for this one-node
topology. Zero-downtime rolling replacement and multi-node failover are not
supported. See [`upgrade_and_rollback.md`](upgrade_and_rollback.md) and deferred
issue [#529](https://github.com/eirhop/favn/issues/529).
