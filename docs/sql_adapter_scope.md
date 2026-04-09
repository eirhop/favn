# SQL Adapter Scope

## Purpose

This document defines the scope for Favn's first SQL adapter foundation in v0.4.0.

The goal is to make the first SQL workflow possible on top of Favn's existing asset, planner, runtime, scheduler, and windowing model without over-designing for every future backend or connector type.

This document exists to keep v0.4 focused, reduce architectural drift, and make later adapter work incremental rather than speculative.

---

## Summary

v0.4.0 should introduce a **narrow and stable SQL foundation** for Favn.

We will **not** try to solve all "data platform adapters" in one abstraction.

Instead, we will split responsibilities into clear layers:

1. **`Favn.Connection`**  
   Validated reusable connection configuration and registration metadata.

2. **`Favn.SQL.Adapter`**  
   Backend-specific SQL engine behavior for introspection, execution, and materialization.

3. **Future: `Favn.Source.Adapter`**  
   External source connector behavior such as pagination, checkpoints, bookmarks, rate limits, and extract semantics.

This means v0.4 focuses on **SQL engines**, not on generic API/file/source connectors.

---

## Why this scope

SQL engines and source connectors have very different responsibilities.

A SQL adapter needs to answer questions like:

- how to connect
- how to inspect schemas and relations
- how to execute SQL
- how to materialize views and tables
- how to support incremental behavior
- what features the backend supports

A source connector needs to answer very different questions like:

- how to paginate
- how to checkpoint state
- how to resume extraction
- how to partition streams
- how to handle rate limits
- what delivery guarantees exist

Trying to merge these into one universal adapter contract in v0.4 would make the first implementation too broad, too vague, and too hard to keep clean.

---

## v0.4 Goal

The v0.4 goal is:

> Make the first complete SQL asset workflow possible in Favn using a clean SQL adapter contract and DuckDB as the first backend.

That means v0.4 should enable:

- reusable named SQL connections
- a stable SQL adapter behavior
- backend capability discovery
- relation introspection
- SQL execution
- SQL materialization primitives
- incremental execution foundation
- SQL assets that compile into canonical `%Favn.Asset{}`
- planning and execution on the existing shared runtime/window model

---

## Design principles

### 1. Keep the public contract narrow
We should define the minimum stable adapter behavior needed for SQL assets to work well.

### 2. Model capabilities explicitly
Favn must not assume every SQL backend supports the same behavior.
Features should be declared through capabilities, not guessed from adapter names.

### 3. Separate connection from adapter
Connection config and connection lifecycle are not the same thing as SQL execution behavior.

### 4. Normalize Favn concepts, not backend terminology
Favn should expose a consistent internal model for catalog/schema/relation/materialization even when backend terminology differs.

### 5. Prefer additive evolution
The first adapter contract should be small enough that later backends can extend it without breaking the first implementation.

### 6. Reuse the existing runtime model
SQL assets must fit the same planner, run, retry, backfill, and windowing model already established for Elixir assets.

---

## In scope for v0.4

## 1. `Favn.Connection`

Introduce a reusable connection model for SQL backends.

### Responsibilities

- define a named connection
- validate configuration through an explicit field schema
- support required, optional, defaulted, secret, and typed fields
- provide a stable connection reference that assets can depend on indirectly

### Requirements

- configuration validation must fail fast
- connection definitions must declare allowed keys explicitly (schema-driven strict mode)
- secrets must not leak in logs or runtime metadata
- connection definitions must be reusable across multiple SQL assets
- host applications must be able to register one or more SQL connections
- connection usage should be explicit in the asset or module definition

### Notes

`Favn.Connection` should be backend-agnostic enough to work for multiple SQL engines, but it does **not** need to solve generic source connector config in v0.4.

Startup boundary for v0.4:

- SQL asset compile/discovery must not depend on resolved runtime connection payloads.
- SQL assets may reference connection **names** during compile/discovery.
- Resolved connection values are required at runtime/execution boundaries.
- Connection lifecycle callbacks (`connect`, `disconnect`, `ping`) belong to
  `Favn.SQL.Adapter`, not `Favn.Connection`.

---

## 2. `Favn.SQL.Adapter`

Introduce the core SQL engine behavior contract.

### Responsibilities

- expose backend capabilities
- introspect schemas and relations
- execute SQL statements
- query result sets when needed
- provide materialization operations
- define backend-specific incremental behavior support
- normalize backend errors into predictable adapter errors

### Minimum required behavior

#### Connection lifecycle
- connect
- disconnect / dispose
- ping / health check

#### Capability discovery
- supports views
- supports tables
- supports replace semantics
- supports transactions
- supports merge / upsert
- supports materialized views
- supports comments
- supports metadata timestamps
- supports query tracking

Not every backend must support every capability, but every adapter must report what it supports.

#### Introspection
- schema exists?
- relation exists?
- relation type
- list schemas
- list relations in schema
- fetch columns for relation

This is the minimum introspection needed to support planning, materialization decisions, and dependency reasoning.

#### Execution
- execute statement
- execute statements in sequence when needed
- query rows or scalar metadata when needed
- return structured execution results where useful

#### Materialization
- create schema if missing
- create view
- replace view when supported
- create table as select
- replace or rebuild table when needed
- append into table
- truncate and rebuild when needed

#### Incremental foundation
v0.4 should support the **foundation** for incremental SQL assets, not every possible strategy.

At minimum Favn should model these strategy families:

- `:append`
- `:replace`
- `:delete_insert`
- `:merge` (only when backend supports it)

#### Errors
The adapter layer must not return only opaque backend exceptions.

Favn should be able to distinguish at least:

- invalid config
- authentication error
- connection error
- retryable execution error
- non-retryable SQL error
- unsupported capability
- introspection mismatch / missing relation

---

## 3. SQL relation model

Introduce a normalized internal relation model.

### Needed concepts

- catalog
- schema
- relation name
- relation type
- quoted / rendered identifier
- canonical identity for dependency reasoning

This model should belong to Favn rather than leaking raw backend-specific naming rules into the rest of the planner/runtime.

---

## 4. SQL materialization model

Introduce a first-class Favn materialization model for SQL assets.

### Initial materializations in scope

- `:view`
- `:table`
- `:incremental`

### Requirements

- the materialization must be part of canonical asset metadata
- the planner/runtime must not care whether the asset is written in Elixir or SQL
- adapter behavior should map Favn materializations to backend-specific execution safely
- unsupported materializations must fail explicitly

---

## 5. SQL asset compilation

v0.4 should use the existing asset compiler seam to allow SQL-authored assets to compile into canonical `%Favn.Asset{}` values.

### In scope

- SQL asset authoring model
- SQL assets compile into canonical asset metadata
- dependencies are represented as canonical refs
- SQL assets can participate in graph planning like Elixir assets

### Not required for first cut

- final polished authoring DSL
- full macro ergonomics
- every possible SQL authoring style

The important thing in v0.4 is the **compiled canonical shape**, not the final syntax sugar.

---

## 6. Dependency inference for SQL assets

v0.4 should support dependency inference from Favn-aware SQL references.

### In scope

- dependencies declared or inferred through a typed Favn relation/reference model
- dependency graph integration with the existing planner
- deterministic canonical dependency resolution

### Out of scope for v0.4

- arbitrary SQL parser completeness
- full SQL lineage extraction for every query form
- dialect-complete parsing across all engines

For v0.4, dependency inference should be pragmatic and explicit enough to stay reliable.

---

## 7. Window-aware SQL execution

SQL assets must work on the same window-aware runtime model as Elixir assets.

### In scope

- SQL assets may declare window intent
- SQL assets receive runtime window context
- SQL execution can parameterize window-aware queries/materializations
- backfills and anchor-window runs use the same planner/runtime semantics already established in Favn

### Important note

The adapter should not invent a parallel scheduling/runtime model for SQL assets.
SQL assets must fit into the existing Favn runtime model.

---

## 8. DuckDB as the first implementation

DuckDB is in scope as the first backend implementation of `Favn.SQL.Adapter`.

### Why DuckDB first

- small operational surface
- strong local developer experience
- good fit for single-node early Favn usage
- useful as the reference implementation for materialization and incremental semantics

### Expectations

DuckDB should be treated as the **first adapter**, not as the architecture.

The SQL adapter contract must not be shaped around DuckDB-only assumptions that would make later adapters awkward.

---

## Explicitly out of scope for v0.4

The following are intentionally outside scope.

## 1. Generic source connector abstraction

Not in v0.4:

- REST API connectors
- file source connectors
- CDC connectors
- extract stream abstractions
- generic "read from anything" adapter behaviors

These belong to a future `Favn.Source.Adapter`.

---

## 2. Pagination and checkpoint state

Not in v0.4:

- cursor pagination
- page token strategies
- bookmark state
- extract resume semantics
- per-stream state partitions

These are source connector concerns, not SQL engine concerns.

---

## 3. Rate limiting and delivery guarantees

Not in v0.4:

- rate limit handling
- throttling policies for API connectors
- at-least-once / exactly-once extract guarantees
- source deduplication guarantees

Again, these belong to future source adapters.

---

## 4. Multi-backend completeness

Not in v0.4:

- Snowflake adapter
- Postgres adapter
- Databricks adapter
- BigQuery adapter
- Trino adapter
- full dialect abstraction across many engines

v0.4 only needs the contract and one real implementation: DuckDB.

---

## 5. Full SQL parser / lineage engine

Not in v0.4:

- complete SQL lineage extraction
- complex dialect-complete parsing
- automatic dependency inference from arbitrary freeform SQL
- optimizer-level query rewrites

Dependency inference should stay controlled and predictable in the first release.

---

## 6. Advanced physical design features

Not in v0.4 unless needed by DuckDB in a minimal way:

- indexing abstraction
- clustering abstraction
- partitioning abstraction for all warehouses
- sort keys
- table distribution keys
- warehouse-native storage tuning APIs

These can be added later through capability-specific extensions if needed.

---

## 7. Multi-node / distributed SQL execution concerns

Not in v0.4:

- distributed placement rules for SQL engine workloads
- remote query compute planning
- resource-aware node scheduling
- cross-node execution routing

That work belongs to later distributed Favn milestones, not the first SQL adapter foundation.

---

## Proposed module surface

This is a proposed starting structure, not a final locked tree.

```text
lib/favn/
  connection.ex
  connection/
    definition.ex
    registry.ex

  sql/
    adapter.ex
    capabilities.ex
    relation.ex
    materialization.ex
    compiler.ex

    adapter/
      duckdb.ex
````

Optional later additions:

```text
lib/favn/sql/
  introspection.ex
  errors.ex
  query.ex
  asset.ex
```

---

## Proposed phased delivery inside v0.4

## Phase 1 — Connection and adapter contract

Deliver:

* `Favn.Connection`
* `Favn.SQL.Adapter`
* `Favn.SQL.Capabilities`
* `Favn.SQL.Relation`
* adapter error model
* adapter conformance tests

This phase defines the architecture.

---

## Phase 2 — DuckDB adapter

Deliver:

* DuckDB connection wiring
* capability declaration
* introspection
* SQL execution
* materialization primitives

This phase proves the contract.

---

## Phase 3 — SQL asset compilation

Deliver:

* SQL asset authoring model
* compilation into canonical `%Favn.Asset{}`
* materialization metadata
* connection binding
* dependency declaration / inference foundation

This phase makes SQL assets first-class in the catalog.

---

## Phase 4 — Window-aware and incremental execution

Deliver:

* SQL assets on shared runtime window model
* incremental strategy foundation
* lookback-aware execution semantics
* backfill support through existing runtime/planner

This phase makes SQL assets practically useful.

---

## Non-goals

The following are non-goals for this document:

* define the full end-user SQL DSL syntax
* define every future adapter extension point
* define UI concerns for `favn_view`
* define distributed execution architecture
* define generic ETL connector plugin architecture

This document is specifically about scoping the first SQL adapter foundation.

---

## Key decisions

### Decision 1

We will **not** build one universal "data platform adapter" for v0.4.

### Decision 2

We will split responsibilities into:

* `Favn.Connection`
* `Favn.SQL.Adapter`
* future `Favn.Source.Adapter`

### Decision 3

DuckDB is the first implementation, but not the architecture.

### Decision 4

Capabilities must be explicit.
Favn must not assume feature parity across backends.

### Decision 5

SQL assets must compile into canonical `%Favn.Asset{}` and run on the existing shared runtime model.

---

## Open questions

These should be resolved during implementation design.

### 1. Connection registration

How should host applications register SQL connections?
Examples:

* config-based
* module-based
* runtime registry
* explicit `use Favn.Connection`

### 2. Connection reference model

Should SQL assets refer to:

* a named connection atom
* a module-backed connection definition
* a canonical connection ref struct

### 3. SQL asset authoring

What is the first authoring shape?
Examples:

* inline SQL
* external `.sql` file
* multi-asset SQL module
* explicit typed source declarations

### 4. Dependency inference strategy

How much inference should be automatic in v0.4 versus explicitly declared?

### 5. Incremental API

Should incremental behavior be defined as:

* materialization options
* strategy modules
* adapter callbacks
* planner/runtime metadata

### 6. Adapter test contract

What behavior must every SQL adapter prove to be considered conformant?

---

## Final scope statement

v0.4 is about building the **first clean SQL foundation** for Favn.

It is **not** about solving every external connector pattern.

If a behavior is required to support:

* reusable SQL connections
* SQL relation introspection
* SQL execution
* SQL materialization
* SQL asset compilation
* window-aware SQL asset execution
* DuckDB as the first backend

then it is in scope.

If a behavior is mainly about:

* source extraction
* pagination
* checkpoint state
* bookmarks
* rate limits
* stream partitioning
* generic connector delivery guarantees

then it is out of scope for v0.4 and belongs to a future source adapter layer.


---

## DuckDB implementation request

### Executive recommendation

Adopt **duckdbex now behind a strict internal Favn DuckDB client boundary**.

This should be treated as a replaceable internal implementation detail used to prove the
`Favn.SQL.Adapter` architecture with a real backend in v0.4, not as a public dependency contract.

### Why this decision now

- It provides a working path for core v0.4 needs (open DB, connect, execute/query with parameter binding, introspection, appender, transaction helpers).
- It lets Favn validate its SQL adapter architecture with a concrete runtime backend instead of only abstract contracts.
- It keeps replacement cost bounded by isolating duckdbex-specific resource/NIF behavior in one internal module family.
- It avoids building and maintaining a bespoke NIF client too early while Favn still needs to prove SQL runtime semantics.

### Risk profile and boundaries

`duckdbex` is NIF-backed and currently uses Dirty NIF execution.

That has two consequences for Favn:

1. The adapter/client layer must enforce explicit lifecycle discipline (`open/connect`, `release`, `disconnect`) with best-effort cleanup in all execution paths.
2. Favn should isolate NIF-facing calls behind internal boundaries so unexpected crashes or resource leaks are contained and observable at adapter/session boundaries.

The documented amalgamation path also excludes bundled JSON/HTTPFS/SQLite Scanner/Postgres Scanner/Substrait extensions, so v0.4 must not promise those extension capabilities by default.

### Proposed module boundaries

```text
lib/favn/
  sql.ex                         # facade, normalization, orchestration
  sql/
    adapter.ex                   # behaviour contract
    adapter/
      duckdb.ex                  # Favn.SQL.Adapter implementation (DuckDB-facing)
      duckdb/
        client.ex                # internal duckdbex wrapper boundary
        session.ex               # runtime session/resource ownership
        mapper.ex                # duckdbex -> Favn.SQL.Result/Error mapping
        introspection.ex         # fallback SQL for schema/relation/column reads
        materialization.ex       # :view/:table statements + appender write path
```

Boundary rule: `Duckdbex.*` modules and structs must never leak into public `Favn`, `Favn.Connection`, or `Favn.SQL.Adapter` contracts.

### Runtime model fit

Favn should keep DB-handle ownership as a **Phase A implementation decision** validated by lifecycle tests, not a locked architectural commitment in this document.

Initial evaluation targets:

- explicit resource ownership for DB/connection/statement/appender handles
- explicit `release` discipline with best-effort cleanup on all paths
- session patterns that can evolve (for example, shared DB handle + short-lived execution connections, or stricter per-run ownership)
- explicit transaction boundaries at operation scope (not globally shared)

DuckDB concurrency should be described as a **single-process write domain with optimistic conflict handling**. Multiple writes can proceed concurrently within one process when they do not conflict (appends are generally non-conflicting), and write conflicts should be surfaced as explicit execution errors for Favn retry/cancel policy handling.

### v0.4 capability target

Minimum implementation target for the first DuckDB slice:

- open database + create runtime connection
- execute and query with normalized `Favn.SQL.Result`
- fallback introspection support (`schema_exists?`, `relation`, `list_*`, `columns`)
- parameter binding for execution/query paths (prepared statement lifecycle is a later hardening step)
- appender-based bulk write support for table materialization paths
- explicit transaction helpers for grouped materialization operations
- baseline `:view` and `:table` materialization support

Incremental strategies stay foundation-only in this step (`:append` first; other strategies staged behind capability checks).

### Feature request scope (implementation ticket)

#### Objective

Deliver the first production-shaped DuckDB backend for Favn v0.4 using duckdbex behind a replaceable internal client boundary, proving the SQL adapter architecture end-to-end.

#### In scope

- Internal `Favn.SQL.Adapter.DuckDB` using duckdbex client wrapper modules
- Lifecycle-safe connect/disconnect/release behavior
- Query/execute + parameter binding support
- Introspection callbacks + fallback SQL renderer
- Appender-powered bulk write path for table materialization
- `:view` and `:table` materialization statements
- Adapter conformance and DuckDB smoke tests

#### Non-goals

- Public exposure of duckdbex API/types/errors
- Multi-node/distributed DuckDB execution semantics
- Full extension management surface (JSON/HTTPFS/scanners/Substrait)
- Complete incremental strategy matrix in first slice

#### Acceptance criteria

- `Favn.SQL.Adapter` conformance tests pass for DuckDB adapter
- Runtime operations never return raw duckdbex values across Favn public/internal contracts
- Materialization `:view` and `:table` complete through adapter path in integration tests
- Resource lifecycle tests verify release/cleanup on success and failure paths
- docs clearly declare DuckDB extension and concurrency limits for v0.4

#### Risks

- NIF crash/resource leak surface if lifecycle discipline is incomplete
- Runtime contention from optimistic write conflicts
- Environment-specific build/runtime constraints from amalgamation setup

#### Dependencies

- `duckdbex` (target evaluation version: `0.3.21`)
- existing `Favn.Connection` registry/validation
- existing `Favn.SQL.Adapter` + `Favn.SQL` normalization paths

### Phased implementation plan

1. **Phase A — Client boundary + lifecycle guards**
   - Add internal DuckDB client wrapper around duckdbex.
   - Add session/resource ownership model and best-effort cleanup helpers.

2. **Phase B — Execute/query + introspection**
   - Implement adapter required callbacks for execute/query and fallback introspection SQL.
   - Map all errors/results to Favn normalized structs.

3. **Phase C — Materialization baseline**
   - Implement `:view` and `:table` materialization fallback statements.
   - Use appender path for bulk insert-oriented table writes.

4. **Phase D — Transactions + prepared statements + hardening**
   - Wire transaction helper usage for grouped writes.
   - Add explicit prepared statement lifecycle support (`prepare_statement`/`execute_statement`) where it improves runtime safety/performance.
   - Add failure cleanup tests and concurrency conflict smoke coverage.

5. **Phase E — v0.4 documentation gate**
   - Publish setup and lifecycle docs (dependency/build/runtime/release).
   - Publish limitations: dirty NIF usage, extension bundle limits, single-process write model expectations.

### Documentation checklist for this work

1. duckdbex dependency/setup in Favn docs
   - dependency and build prerequisites
   - runtime open/config patterns
   - lifecycle and release expectations
   - explicit version pin documentation (`0.3.21` evaluation target), without copy-pasting older upstream snippets

2. duckdbex usage surface documented for Favn maintainers
   - prepared statement lifecycle (future hardening step)
   - appender
   - transactions
   - cleanup/release patterns
   - pinned library version visibility

3. DuckDB behavior references in Favn docs
   - concurrency model and optimistic conflicts
   - single-process write assumptions
   - appender guidance for bulk ingest

4. Favn user-facing DuckDB adapter docs
   - connection configuration examples
   - how adapter plugs into `Favn.Connection` and `Favn.SQL.Adapter`
   - v0.4 limitations and extension constraints
   - recommended single-node local analytics usage patterns

### Decision gates

Revisit and potentially reject duckdbex for v0.4 if either gate fails:

- **Gate 1 (stability):** adapter conformance/lifecycle tests reveal unacceptable crash/leak behavior under normal retry/cancel/failure flows.
- **Gate 2 (scope fit):** required v0.4 capabilities cannot be implemented without leaking duckdbex details into Favn contracts.

If both gates pass, proceed with duckdbex-backed DuckDB adapter as the v0.4 reference backend implementation.
