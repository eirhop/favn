# Favn Roadmap

PostgreSQL Storage V2 is implemented. The shortest path to a supported production
release is five large, coherent implementation epics. Shipped capability and
current limits live in [`FEATURES.md`](FEATURES.md); release gates live in
[`production/README.md`](production/README.md).

## Production release path

1. [#522 — runnable release artifacts and supported deployment topology](https://github.com/eirhop/favn/issues/522)
   - Ship one reusable Favn control-plane image and one relocatable
     customer-built runner context with an aligned manifest release.
   - Qualify the one-control-plane/one-runner topology, runtime-only
     configuration, environment-only secrets, health, packaging, upgrades, and
     clean-container acceptance.
   - Validate the manifest, runner, and public package boundary through the clean
     canonical customer-project fixture.
   - Project-owned local/single-host Compose templates, typed role validation,
     data-preserving lifecycle ownership, and cached immutable customer-runner
     builds are implemented; remaining #522 work is release qualification and
     publishing evidence.
2. [#525 — durable scheduling and asynchronous orchestration](https://github.com/eirhop/favn/issues/525)
   - Persist submission intent, move work outside scheduler/RunManager critical
     paths, and add bounded workers, recovery, fairness, cancellation, and visibility.
3. [#526 — DuckDB/DuckLake data-plane production hardening](https://github.com/eirhop/favn/issues/526)
   - Define data-plane durability and recovery, add failure injection and honest
     cancellation, and finish safe operator resource controls.
4. [#524 — production operator UI, security, and browser acceptance](https://github.com/eirhop/favn/issues/524)
   - Finish operator workflows, mutation audit, actor/session administration,
     authorization, accessibility, and real-browser release acceptance.
5. [#523 — PostgreSQL production proof and observability](https://github.com/eirhop/favn/issues/523)
   - Prove managed PostgreSQL 18 restore, PITR, load, contention, failover, query
     plans, least privilege, dashboards, alerts, and incident response.

Observability and drill tooling from #523 should start alongside #522; #523 closes
last as release qualification. The initial supported target is one control-plane
node, one separate runner node, and PostgreSQL. Multi-node control-plane/runner
scaling is tracked in [#529](https://github.com/eirhop/favn/issues/529).
Deployments without a fully isolated trusted BEAM network are tracked in
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
- Multi-node control-plane and runner clusters after the single-node roles are
  proven in production.
- Encrypted or least-privilege runner transport, runtime secret providers, and
  automatic rotation for less isolated deployments.
