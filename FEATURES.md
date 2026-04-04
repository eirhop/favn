# Favn Roadmap & Feature Status

## Current Version

**Current release: v0.1.0**

Favn is in private pre-v1 development with a refactor-first roadmap.

This document is the canonical source of truth for roadmap scope and milestone order.

## Product Direction (Canonical)

Favn is an **asset-first ETL/ELT orchestration** library for Elixir.

- Author assets with a compact DSL: `@asset`, `@depends`, `@uses`, `@freshness`, `@meta`
- Author executable work with a single function contract: `def asset(ctx)`
- Treat dependencies as **ordering + lineage**, not in-memory value passing
- Use **external materialization** as the data boundary between assets
- Keep orchestration config outside function attributes and make it available through `ctx`

## Execution order

Roadmap work is prioritized in this order:

1. DSL contract
2. Runtime alignment
3. Orchestration config model
4. v1 triggers/scheduling

## In scope for v1

- Stable asset DSL (`@asset`, `@depends`, `@uses`, `@freshness`, `@meta`)
- Stable runtime asset contract: `def asset(ctx)`
- Deterministic dependency-based execution
- Run/step lifecycle with retry, timeout, cancellation, rerun
- Freshness-aware skip decisions
- External materialization model with lineage and run visibility
- Orchestration config model outside function attributes, accessible via `ctx`

## Out of scope for v1 function DSL

The following stay out of function attributes in v1:

- `schedule`
- `polling`
- `polling cursor/state`
- webhook/api/event trigger definitions
- pipeline installation/config declarations
- data-passing configuration between assets

## Roadmap

## v0.1.0 — Foundation

**Status: Released**

- [x] Asset discovery and registry foundation
- [x] Dependency graph construction (DAG)
- [x] Deterministic planning foundation
- [x] Run model and event foundation
- [x] Public API entrypoint (`Favn`)

---

## v0.3.0 — DSL + Runtime Contract Refactor

**Status: In Progress**

This milestone is the single pre-v1 refactor milestone and current focus.

### DSL contract

- [ ] Canonical DSL: `@asset`, `@depends`, `@uses`, `@freshness`, `@meta`
- [ ] Remove option-style dependency authoring in docs/examples
- [ ] Keep dependencies focused on ordering + lineage

### Runtime alignment

- [ ] Canonical asset contract: `def asset(ctx)`
- [ ] Align execution/runtime APIs to single-arg asset invocation
- [ ] Align examples/docs to external materialization boundaries

### Orchestration config model

- [ ] Keep orchestration config outside function attributes
- [ ] Provide orchestration-installed config through `ctx`
- [ ] Clarify runtime context shape in public docs

---

## v1.0.0 — Stable Asset-Oriented ETL/ELT Orchestrator

**Status: Planned**

v1 release gates include scheduling and polling capabilities, implemented in orchestration layers (not function attributes).

### v1 release gates

- [ ] `schedule`
- [ ] `polling`
- [ ] `polling cursor/state`
- [ ] Manual/API triggers integrated with stable runtime lifecycle
- [ ] Freshness-aware orchestration decisions on stable asset contract
- [ ] Stable operator visibility for graph, runs, lineage, and materializations

---

## Notes

- Favn is intentionally focused on asset-first ETL/ELT orchestration.
- External materialization is the canonical execution/data boundary.
- Scheduling and polling are explicit v1 gates, not pre-v1 DSL concerns.
