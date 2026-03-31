# Favn Roadmap & Feature Status

## Current Version

**Current release: v0.1**

Favn is in an early development stage. The core foundation is implemented, but the system is not yet production-ready.

This document tracks both **direction (roadmap)** and **progress (feature completion)**.

---

## Vision

Favn is a BEAM-native, asset-first orchestrator for business code.

It should make it easy to:

- define business-oriented assets in plain Elixir
- discover and reason about dependencies through code and documentation
- run deterministic workflows from those dependency relationships
- trigger work manually, via API, on schedules, by polling, or from events
- scale from one node to multiple BEAM nodes
- inspect everything through a UI
- extend via plugins
- support ETL/ELT, integrations, workflows, and AI agents

---

## v0.1.0 — Foundation (Current)

**Status: Released**

This release proves the core architecture and programming model.

### Features

- [x] Asset DSL (`@asset`) for defining business logic
- [x] Asset discovery and registry
- [x] Dependency graph construction (DAG)
- [x] Deterministic planning from dependencies
- [x] Local synchronous execution
- [x] Run model with outputs
- [x] Storage abstraction (in-memory)
- [x] Run event emission (PubSub)
- [x] Public API (`Flux` module)
- [x] Host application integration (supervision tree)
- [x] Basic tests across core modules

---

## v0.2.0 — Stateful Execution Foundation

**Status: Planned**

Turns Favn into a real execution engine with durable state and concurrency.

### Features

- [x] Asynchronous run execution
- [x] Parallel execution with bounded concurrency
- [x] Run + step state machine (pending → running → success/failure)
- [x] Retry mechanism (configurable, step-level fixed delay + max attempts + retry classes)
- [x] Cancellation support
- [x] Timeout handling
- [x] SQLite storage adapter
- [x] SQLite monotonic `updated_seq` allocation via `favn_counters` (replaces `run_write_orders` growth path)
- [x] Stable event schema (runs + steps)
- [ ] Telemetry integration
- [ ] Initial materialization/artifact model (replace in-memory-only outputs)
- [x] Separation of coordinator vs executor internally

---

## v0.3.0 — Jobs, Triggers, Single-Node Production

**Status: Planned**

First production-usable version (single node).

### Features

- [ ] Job / launch definition abstraction
- [ ] Asset selection → job execution mapping
- [ ] Manual run trigger
- [ ] API-based trigger
- [ ] Cron/schedule trigger
- [ ] Postgres storage adapter
- [ ] Queueing and admission control
- [ ] Retry policy extensions (backoff + jitter + richer retry classifiers)
- [ ] Run deduplication (run keys)
- [ ] Stable operator APIs (run, cancel, rerun)
- [ ] `favn_view` alpha (graph + runs + schedules)

---

## v0.4.0 — Distributed Execution (BEAM)

**Status: Planned**

Introduces multi-node execution and placement.

### Features

- [ ] Worker registration
- [ ] Worker heartbeats
- [ ] Remote step execution (multi-node)
- [ ] Resource requirements (memory, CPU, labels)
- [ ] Placement strategy (resource-aware scheduling)
- [ ] Distributed coordination (leases)
- [ ] Worker failure detection
- [ ] Retry/requeue on node failure
- [ ] Cross-node retry attempt handoff and recovery semantics
- [ ] Node drain support
- [ ] Supervision strategy for distributed execution

---

## v0.5.0 — Plugin & Connector Platform

**Status: Planned**

Makes Favn extensible and ecosystem-ready.

### Features

- [ ] Storage plugin interface (stable)
- [ ] Trigger plugin interface
- [ ] Runtime/executor plugin interface
- [ ] Connector plugin system
- [ ] Connector registry (schema + docs + config)
- [ ] Secret/config reference model
- [ ] Metadata extensions for assets/jobs
- [ ] Artifact handling plugin interface
- [ ] Machine-readable catalog export (API)

---

## v0.6.0 — Heterogeneous Runtimes & dbt

**Status: Planned**

Supports non-Elixir execution.

### Features

- [ ] Runtime-agnostic asset identity
- [ ] Mixed-runtime planning
- [ ] External runtime execution bridge
- [ ] dbt manifest ingestion
- [ ] dbt asset mapping into catalog
- [ ] dbt run execution via Favn
- [ ] dbt test execution
- [ ] Normalized dbt metadata (runs/tests/artifacts)
- [ ] `favn_dbt` plugin (alpha)

---

## v0.7.0 — Favn View & UX

**Status: Planned**

Turns Favn into a usable product for operators.

### Features

- [ ] Asset catalog UI
- [ ] Graph explorer (dependencies)
- [ ] Run list view
- [ ] Run timeline view
- [ ] Step-level inspection
- [ ] Schedule/trigger UI
- [ ] Connector UI
- [ ] Source/doc linking
- [ ] Operator actions (cancel, rerun)
- [ ] `favn_demo` project

---

## v0.8.0 — Hardening & Release Candidate

**Status: Planned**

Stabilization and production readiness.

### Features

- [ ] Upgrade/migration strategy
- [ ] Compatibility guarantees (core + plugins)
- [ ] Failure recovery testing
- [ ] Performance benchmarks
- [ ] Deployment reference architecture
- [ ] Production documentation
- [ ] Packaging strategy (core + UI + plugins)
- [ ] Single-image deployment option

---

## v1.0.0 — Stable Release

**Status: Planned**

Production-ready orchestrator.

### Features

- [ ] Stable asset model
- [ ] Stable job model
- [ ] Stable trigger model
- [ ] Durable orchestration state
- [ ] Built-in storage (memory, SQLite, Postgres)
- [ ] Local + distributed execution
- [ ] Manual + API + scheduled triggers
- [ ] Event/polling trigger framework
- [ ] Observability (runs, steps, artifacts)
- [ ] `favn_view` stable release
- [ ] Official plugin interfaces
- [ ] At least one external runtime (dbt)
- [ ] Complete documentation
- [ ] Demo project ready for onboarding

---

## Companion Projects

### favn_view

- [ ] Alpha (v0.3)
- [ ] Usable (v0.7)
- [ ] Stable (v1.0)

### favn_demo

- [ ] Initial example (v0.7)
- [ ] Polished onboarding project (v1.0)

### Official Plugins

- [ ] Storage adapters
- [ ] Trigger adapters
- [ ] Connector integrations
- [ ] Artifact handling
- [ ] dbt plugin

---

## Notes

- This roadmap defines **direction**, not exact implementation order.
- Features may evolve, but architectural principles should remain stable.
- Priority is always: **correct architecture > fast feature delivery**.
- Run/step event schema is versioned with `schema_version` (current `1`); additive changes should preserve existing fields and alias compatibility.
