# Favn v0.5 Refactor Roadmap

## Status

Planned

## Summary

v0.5 is no longer a continuation of the current single-app runtime architecture.

Instead, v0.5 is the refactor release that turns Favn into a product with clear boundaries between:

- user-facing authoring (`favn`)
- compute execution (`favn_runner`)
- orchestration/control plane (`favn_orchestrator`)
- operator UI (`favn_view`)

This replaces the previous v0.5 direction, which focused on hardening the current single-node production setup before later introducing distributed execution and view support. The new direction pulls those architectural boundaries forward now. 

## Why this refactor is needed

Current Favn still assumes one shared OTP app that:

- discovers assets and pipelines from loaded modules at runtime
- builds graph/index state in-process
- starts scheduler/runtime/storage together
- executes asset code locally in-process

That architecture is useful as a prototype, but it is not the right production foundation for:

- separate scaling of orchestrator vs runners
- private/internal runner execution
- internet-facing UI without exposing everything else
- manifest version pinning per run
- controlled deployment topologies
- optional execution plugins such as DuckDB, Snowflake, Databricks, etc.    

## v0.5 goals

By the end of v0.5, Favn should support:

1. **A monorepo umbrella structure** with clear app boundaries.
2. **One public user dependency**: `favn`.
3. **Compiled manifest generation** from user business code.
4. **Manifest-version-pinned runs**.
5. **A separate orchestrator runtime** that operates from persisted manifest data, not from loading user modules directly.
6. **A separate runner runtime** that executes user asset code.
7. **A separate view runtime** for UI/API.
8. **Local developer UX** through `mix favn.dev`, with memory storage by default and SQLite optional.
9. **Single-node packaging** and **split deployment packaging** as supported product modes.
10. **A legacy cutover path** so the old monolith can be retired cleanly.

## v0.5 non-goals

The following are explicitly out of scope for the first refactor phase unless required by the new boundaries:

- perfect multi-node scheduling
- advanced queueing/admission control
- cloud-native autoscaling
- full plugin ecosystem beyond proving one optional runner plugin path
- aggressive Hex publishing of internal apps
- broad feature expansion on top of the old monolith
- polishing every old roadmap item before refactoring

## Target app structure

The umbrella should stay intentionally small.

### Public-facing package
- `favn`
  - the single dependency user business projects add
  - owns public DSL and user-facing integration surface
  - may depend internally on core/compiler/runtime glue

### Internal runtime apps
- `favn_core`
  - internal domain/compiler/manifest foundation
- `favn_runner`
  - runner runtime and execution protocol
- `favn_orchestrator`
  - control plane, scheduling, run lifecycle, manifest storage integration
- `favn_view`
  - Phoenix/LiveView UI and external API

### Adapter/plugin apps
- `favn_storage_postgres`
- `favn_storage_sqlite`
- `favn_duckdb`

### Internal support apps
- `favn_test_support`
- `favn_legacy` (temporary during migration)

## Publication strategy

### Publish on Hex
- `favn`
- `favn_duckdb`
- later other optional runner plugins as they stabilize

### Keep internal for now
- `favn_core`
- `favn_runner`
- `favn_orchestrator`
- `favn_view`
- `favn_storage_postgres`
- `favn_storage_sqlite`
- `favn_test_support`
- `favn_legacy`

Rationale:
users should not need to understand or depend directly on internal product boundaries. Only the user-facing library and optional execution plugins should be public initially.

## Supported deployment modes

### 1. Local development
Driven from the user project with:

- `mix favn.dev`
- `mix favn.dev --sqlite`

Default local behavior:

- memory storage by default
- SQLite optional for persistence
- local orchestrator and view started from version-matched runtime artifacts
- local runner started from the user project

### 2. Single-node deployment
One assembled image containing:

- orchestrator runtime
- view runtime
- runner payload
- user business code
- compiled manifest
- selected plugins

### 3. Split deployment
Three-way topology:

- prebuilt `favn_view`
- prebuilt `favn_orchestrator`
- user-built runner image

This remains the target production contract even if same-node/simple modes are supported.

## Architectural principles for v0.5

1. **Manifest first**
   - orchestration must operate on persisted manifest versions
   - loading user modules at runtime in the orchestrator is legacy behavior to remove

2. **Runner protocol is the product boundary**
   - same-node execution may exist internally
   - but orchestration must target a runner contract, not direct local module invocation

3. **View is internet-facing, orchestrator and runners are private by default**
   - avoid collapsing back into one exposed runtime

4. **One user dependency**
   - user business projects should depend on `favn`, not on internal apps directly

5. **Minimal app count**
   - split only where there is a real deployment, dependency, or ownership boundary

6. **Legacy monolith is reference-only during migration**
   - no major new feature work there
   - use it to preserve behavior and compare outputs

## Phase roadmap

---

## Phase 0 — Freeze and reframe

### Goal
Stop feature drift on the old architecture and establish the new direction.

### Deliverables
- mark old v0.5 roadmap as replaced
- add this roadmap to the repo
- define umbrella app list
- define allowed dependency directions between apps
- define migration rules
- define deletion criteria for legacy code

### Exit criteria
- app list is locked
- dependency rules are locked
- refactor is the official v0.5 direction

---

## Phase 1 — Create the umbrella and isolate legacy

### Goal
Create a safe structure for AI-assisted migration without refactoring in place.

### Deliverables
- create umbrella root
- scaffold:
  - `favn`
  - `favn_core`
  - `favn_runner`
  - `favn_orchestrator`
  - `favn_view`
  - `favn_storage_postgres`
  - `favn_storage_sqlite`
  - `favn_duckdb`
  - `favn_test_support`
  - `favn_legacy`
- move current monolith code into `favn_legacy`
- treat `favn_legacy` as read-only reference

### Exit criteria
- umbrella compiles
- legacy app remains runnable as reference
- no new work happens in root `/lib` and `/test`

---

## Phase 2 — Move public DSL and domain/compiler foundation

### Goal
Establish the stable authoring foundation before moving runtime concerns.

### Deliverables
Move into `favn_core` and/or `favn`:

- public DSL entrypoints
- asset/pipeline/schedule definitions
- canonical structs and types
- graph/dependency planning
- compile-time metadata capture
- manifest data model and manifest generation

`favn` becomes the public facade package that user projects depend on.

### Exit criteria
- user business code can compile against `favn`
- manifest can be generated from user modules without orchestrator/runtime coupling
- unit tests for DSL/compiler/domain run under the new apps

---

## Phase 3 — Define and implement manifest versioning

### Goal
Make manifest versioning the source of truth for orchestration.

### Deliverables
- canonical manifest schema
- version identity strategy
- manifest persistence contract for orchestrator
- run pinning to manifest version
- compatibility checks between manifest and runner runtime

### Exit criteria
- a run can be created against a specific manifest version
- updating manifests does not affect in-flight runs
- orchestrator can reason about manifests without loading user business modules directly

---

## Phase 4 — Build the runner boundary

### Goal
Separate execution from orchestration.

### Deliverables
In `favn_runner`:

- runner runtime supervision
- manifest loading for execution
- asset invocation from manifest-backed resolution
- runner protocol server
- same-node local runner mode for simple/dev flows
- connection/runtime value resolution on the runner side

### Exit criteria
- runner can execute user assets without orchestrator loading the asset modules itself
- orchestrator-facing execution seam exists as a protocol, not only a direct function call
- same-node mode still works through the runner boundary

---

## Phase 5 — Build the orchestrator boundary

### Goal
Make orchestrator the control plane operating on persisted state and runner protocol.

### Deliverables
In `favn_orchestrator`:

- run lifecycle management
- scheduling from persisted manifest versions
- runner dispatch client
- run status transitions
- cancellation/retry hooks
- storage contract for orchestrator state
- internal memory storage for fast local dev

### Exit criteria
- orchestrator no longer depends on runtime user-module discovery for normal operation
- orchestrator can create and track runs against manifest versions
- orchestrator communicates with runner through the new boundary

---

## Phase 6 — Add storage adapters

### Goal
Support realistic persistence modes without polluting core apps.

### Deliverables
- `favn_storage_sqlite`
- `favn_storage_postgres`

Postgres should carry forward the production-oriented transactional/persistence model already being designed, but applied to the new orchestrator architecture rather than the old monolith. :contentReference[oaicite:7]{index=7}

### Exit criteria
- `mix favn.dev` works with memory by default
- `mix favn.dev --sqlite` works with persistent local state
- orchestrator can run with Postgres adapter for production-oriented setups

---

## Phase 7 — Move DuckDB into a runner plugin

### Goal
Prove the optional execution plugin model.

### Deliverables
- `favn_duckdb` app
- DuckDB runtime execution through runner plugin integration
- plugin loading/configuration path through `favn`
- initial public plugin packaging strategy

### Exit criteria
- DuckDB is no longer mixed into the core/orchestrator boundary
- a user can depend on `favn` + `favn_duckdb`
- runner executes SQL/DuckDB functionality through the plugin path

---

## Phase 8 — Add view runtime

### Goal
Create the UI as a true external boundary.

### Deliverables
In `favn_view`:

- Phoenix/LiveView app
- auth/session boundary
- API integration with orchestrator
- manifest/run inspection UI
- local dev integration with `mix favn.dev`

### Exit criteria
- view runs without requiring user projects to add it as a normal Mix dependency
- view interacts with orchestrator APIs, not internal module calls into user code
- local dev can inspect compiled manifests and runs in the UI

---

## Phase 9 — Developer tooling and packaging

### Goal
Make the new architecture usable, not just technically correct.

### Deliverables
- `mix favn.install`
- `mix favn.dev`
- `mix favn.dev --sqlite`
- `mix favn.stop`
- `mix favn.reset`
- `mix favn.build.runner`
- `mix favn.build.single`

Tooling behavior:

- local dev downloads version-matched runtime artifacts for orchestrator/view
- local dev does not require Docker by default
- Docker/dev-container mode may be added optionally later
- single-image assembly combines runtime artifacts with user runner payload and manifest

### Exit criteria
- user can go from `{:favn, ...}` dependency to local UI and orchestration quickly
- user can package single-node and split deployments without understanding internal app structure

---

## Phase 10 — Cut over and delete legacy

### Goal
Retire the old monolith safely.

### Deliverables
- remove remaining runtime dependence on `favn_legacy`
- migrate docs to new architecture
- migrate CI to umbrella layout
- delete root legacy `/lib` and `/test` structure
- eventually remove `favn_legacy`

### Exit criteria
- all supported flows run on the new architecture
- docs describe the new product shape
- legacy app is no longer required for build/test/runtime

## Migration rules

During v0.5 refactor:

1. Do not refactor the old monolith in place.
2. Move code by bounded vertical slice, not random file copying.
3. Each moved slice must bring or recreate its tests.
4. `favn_legacy` is reference-only except for emergency fixes.
5. New architecture decisions override old roadmap assumptions.

## Suggested work order inside the codebase

Recommended migration order:

1. umbrella + legacy isolation
2. `favn_core` / `favn` DSL + compiler + manifest
3. manifest versioning
4. `favn_runner`
5. `favn_orchestrator`
6. storage adapters
7. `favn_duckdb`
8. `favn_view`
9. tooling and packaging
10. legacy deletion

This order minimizes wasted work by establishing the core contracts before moving UI and deployment polish.

## What “done” means for v0.5

v0.5 is complete when:

- user projects depend on `favn` as the public library
- manifests are compiled and versioned
- orchestrator schedules from persisted manifest state
- runner executes user code behind a runner boundary
- view is a separate runtime
- local dev works through `mix favn.dev`
- SQLite local persistence works
- single-node packaging works
- split deployment works in first supported form
- old monolith is no longer the active architecture

## Nice-to-have items after v0.5

These should come after the refactor foundation, not before:

- stronger queueing/admission control
- resource-aware placement
- advanced multi-node scheduling
- richer plugin catalog
- separate plugin repo if ecosystem size justifies it
- advanced cloud deployment automation
- deeper operator tooling and observability polish