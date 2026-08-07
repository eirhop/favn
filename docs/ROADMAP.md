# Favn Roadmap

PostgreSQL Storage V2 is implemented. The shortest path to a supported production
release is five large, coherent implementation epics. Shipped capability and
current limits live in [`FEATURES.md`](FEATURES.md); release gates live in
[`production/README.md`](production/README.md).

## Production release path

1. [#522 — runnable release artifacts and supported deployment topology](https://github.com/eirhop/favn/issues/522)
   - Ship one reusable Favn control-plane image, an editable customer-owned
     runner template, and the explicit release-ID manifest interface.
   - Qualify the one-control-plane/elastic-runner topology, runtime-only
     configuration, environment-only secrets, health, packaging, upgrades, and
     clean-container acceptance.
   - Validate the manifest, runner, and public package boundary through the clean
     canonical customer-project fixture.
   - A direct control-plane build, customer-owned single-host deployment
     example, explicit external PostgreSQL, and runner/manifest identity
     boundary are implemented; remaining #522 work is target deployment
     qualification and publishing evidence.
2. [#525 — durable scheduling and asynchronous orchestration](https://github.com/eirhop/favn/issues/525)
   - Durable run submissions and durable runner tasks are implemented as
     distinct queue contracts: control-plane workers consume the first and
     compatible runners consume the second.
   - The same implementation adds bounded workers, recovery, fairness,
     cancellation, visibility, arbitrary user-defined runner pools, numeric
     capacity demand, release coexistence, and elastic self-exit. Final
     production qualification remains part of the
     [one-control-plane elastic-runner program](architecture/elastic-runners.md).
   - Remaining recovery hardening includes reconstructing already-settled
     sibling statuses after a mid-stage control-plane crash and batching active
     runner-task recovery reads. Load qualification must include concurrent
     checkpoint decoding against the node-wide active-run memory budget.
3. [#526 — DuckDB/DuckLake data-plane production hardening](https://github.com/eirhop/favn/issues/526)
   - Define data-plane durability and recovery, add failure injection and honest
     cancellation, and finish safe operator resource controls.
4. [#524 — production operator UI, security, and browser acceptance](https://github.com/eirhop/favn/issues/524)
   - Finish operator workflows, mutation audit, actor/session administration,
     authorization, accessibility, and real-browser release acceptance.
   - The UI plan, its measured findings, and the order of work are in
     [`design/operator-ux-review.md`](design/operator-ux-review.md).
   - [#580](https://github.com/eirhop/favn/issues/580) now has a first,
     deliberately narrow external-auth adapter: single-tenant Microsoft Entra
     through Azure Container Apps Easy Auth, with durable explicit actor links.
     Generic OIDC and additional providers remain future work.
5. [#523 — PostgreSQL production proof and observability](https://github.com/eirhop/favn/issues/523)
   - Prove managed PostgreSQL 18 restore, PITR, load, contention, failover, query
     plans, least privilege, dashboards, alerts, and incident response.
   - [#588](https://github.com/eirhop/favn/issues/588) implements the
     passwordless control-plane connection, release-operation, and Azure
     reference path with separate runtime/migration managed identities. The
     long-duration live Azure acceptance and managed-service evidence remain a
     #523/#578 release gate.
   - [#611](https://github.com/eirhop/favn/issues/611) implements one idempotent
     PostgreSQL bootstrap Job plus read-only status and least-privilege upgrade,
     including exact Azure identity mapping and a provider-neutral password
     path. Target-Azure first-install, transition, and temporary-authority cleanup
     evidence remains part of the same managed-service qualification gate.

Observability and drill tooling from #523 should start alongside #522; #523 closes
last as release qualification. The initial supported target is one control-plane
node, PostgreSQL, and zero to N runners across arbitrary user-defined pools.
A maximum of one resident runner remains a supported deployment configuration,
not a separate architecture. The runner-scaling part of
[#529](https://github.com/eirhop/favn/issues/529) will be delivered in the same
implementation PR as #525; multi-control-plane availability remains open as
later work.

Production runner connections will use mutually authenticated TLS distributed
BEAM inside a private network, while retaining distributed BEAM's broad peer
trust.
Least-privilege or untrusted-runner transport and broader automatic
credential-rotation work remain tracked in
[#530](https://github.com/eirhop/favn/issues/530).

The first supported release is ready only when these epics are complete or a
remaining item is explicitly removed from the supported product contract.

## Later

- Standalone, manually approved SQL asset migrations are tracked in
  [#533](https://github.com/eirhop/favn/issues/533). They follow the target-generation,
  locking, validation, activation, audit, and unknown-outcome contracts proven by
  [#532](https://github.com/eirhop/favn/issues/532); migrations remain separate
  authored operations rather than SQL asset macros.
- Optional PostgreSQL row-level security as defense in depth.
- Additional SQL adapters and credential providers driven by customer demand.
- A smaller development-only storage adapter only if PostgreSQL developer-loop
  measurements justify a second implementation.
- Native Windows CI when Windows becomes a supported platform.
- Multi-control-plane availability after one-control-plane elastic runners are
  proven in production.
- Least-privilege runner transport that does not grant full BEAM peer trust,
  runtime secret providers, and automatic rotation for less isolated
  deployments.
