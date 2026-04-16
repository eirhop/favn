# Favn v0.5 Refactor Roadmap

## Status

Phase 3 through Phase 8 are implemented (with later-phase follow-ups tracked in `docs/FEATURES.md` and per-phase TODO files)

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

## Target steady-state architecture (explicit)

This is the intended end-state product boundary and should drive all Phase 3+
decisions:

- `favn`: public authoring dependency (public DSL + public facade)
- `favn_core`: internal shared compiler/manifest/planning/contracts layer
- `favn_runner`: execution/runtime boundary
- `favn_orchestrator`: control plane and system of record (persisted manifests, scheduling, run lifecycle)
- `favn_view`: UI/API integration through orchestrator APIs only

Critical runtime rule:

- orchestrator and runner must operate on persisted manifest data and pinned manifest versions
- orchestrator must not discover/load user business modules at runtime
- runner executes pinned manifest-backed work, not ad hoc runtime module discovery

### Public-facing package
- `favn`
  - the single dependency user business projects add
  - owns public DSL and user-facing integration surface
  - compiles manifests from user business code
  - depends on `favn_core`, but not on runtime product apps directly

### Internal runtime apps
- `favn_core`
  - internal DSL/compiler/manifest foundation and shared boundary contracts
- `favn_runner`
  - lightweight runtime wrapper around user business code execution
  - loads pinned manifests, executes assets, and emits execution results/events
- `favn_orchestrator`
  - control plane and product API boundary
  - owns scheduling, run lifecycle, manifests, event history, and storage contracts
- `favn_view`
  - Phoenix/LiveView UI
  - reads and writes through orchestrator APIs only

### Adapter/plugin apps
- `favn_storage_postgres`
  - orchestrator storage adapter
- `favn_storage_sqlite`
  - orchestrator storage adapter
- `favn_duckdb`
  - runner-side execution plugin

### Internal support apps
- `favn_test_support`
- `favn_legacy` (temporary during migration)

## Phase 0 decisions

The following decisions are the Phase 0 deliverable answers and are treated as locked unless a later refactor decision explicitly replaces them.

### Old v0.5 roadmap replacement

- The previous `docs/FEATURES.md` v0.5 section about hardening the current single-app runtime is replaced by this refactor roadmap.
- Already-completed prerequisites from the old roadmap remain valid history.
- Not-yet-completed old roadmap items are deferred until after the new boundaries exist, unless one becomes necessary to unblock the refactor itself.

### Locked umbrella app list

The target app structure above is the locked Phase 0 umbrella app list:

- public package: `favn`
- internal runtime apps: `favn_core`, `favn_runner`, `favn_orchestrator`, `favn_view`
- adapter/plugin apps: `favn_storage_postgres`, `favn_storage_sqlite`, `favn_duckdb`
- internal support apps: `favn_test_support`, `favn_legacy`

No additional umbrella apps should be introduced during early migration unless the boundary cannot be expressed cleanly with this set.

### Allowed dependency directions between apps

Allowed compile-time dependency directions for the umbrella:

- `favn -> favn_core`
- `favn_orchestrator -> favn_core`
- `favn_runner -> favn_core`
- `favn_view -> favn_orchestrator`
- `favn_storage_postgres -> favn_orchestrator`
- `favn_storage_sqlite -> favn_orchestrator`
- `favn_duckdb -> favn_runner`
- `any umbrella app -> favn_test_support`, but only with `only: :test`

Locked dependency rules:

1. `favn_core` must stay at the bottom of the graph and must not depend on any other umbrella app.
2. `favn` is the public authoring package and must not depend on `favn_runner`, `favn_orchestrator`, `favn_view`, storage adapters, plugins, or `favn_legacy`.
3. `favn_core` should contain only DSL/compiler/manifest concerns plus truly shared boundary contracts, not scheduler, storage, or UI behavior.
4. `favn_orchestrator` is the system of record and must own manifests, schedules, runs, event history, operator APIs, and storage contracts.
5. `favn_orchestrator` must not depend on `favn_runner` implementation details; any runner protocol contract shared across runtimes belongs in `favn_core`.
6. `favn_runner` is execution-only and must not depend on `favn_orchestrator`, storage adapters, or `favn_view`.
7. `favn_runner` should execute pinned manifest work, invoke runner-side plugins, and emit execution results/events back to orchestrator.
8. `favn_view` must depend on `favn_orchestrator` APIs only and must not depend directly on `favn_core`, `favn_runner`, storage adapters, plugins, or user business code.
9. Storage apps depend only on `favn_orchestrator`; storage is a control-plane concern.
10. Runner plugins depend only on `favn_runner`; execution plugins are a runner concern.
11. No production app may depend on `favn_test_support`.
12. Any umbrella app may depend on `favn_test_support` only as a test-only dependency.
13. `favn_test_support` should hold only cross-app test fixtures, helpers, builders, and file fixtures.
14. App-specific fixtures should stay in `apps/<app>/test/support`.
15. `favn_test_support` must stay dependency-light and must not take umbrella-app dependencies that would prevent low-level apps such as `favn_core` from using it in tests.
16. No new app may depend on `favn_legacy`.

### Packaging rule for `favn_runner`

- `favn_runner` is part of product packaging, but not part of the user project's public dependency surface.
- In local dev and single-node packaging, the runner runtime will still be bundled together with the user's business code.
- In split deployment, the runner image/payload is built with user business code plus the internal runner runtime.
- The key distinction is packaging versus authoring dependency: users package runner runtime artifacts, but they should not add `favn_runner` as a normal public Mix dependency to author assets.

### Locked migration rules

During v0.5 refactor:

1. Do not refactor the old monolith in place.
2. Move code by bounded vertical slice, not random file copying.
3. Each moved slice must bring or recreate its tests.
4. `favn_legacy` is reference-only except for emergency fixes.
5. New architecture decisions override old roadmap assumptions.
6. Public docs and examples must move with the new source of truth as slices are migrated.
7. New feature work should land in the new architecture unless it is a blocker-level fix for legacy behavior.
8. Shared test fixtures needed across owner apps should move into `favn_test_support` and be consumed via test-only dependencies.
9. Fixtures used by only one app should remain local under that app's `test/support`.

### Namespace ownership handoff (Phase 1 -> Phase 2)

To prevent `Favn.*` collisions during migration:

1. During Phase 1 scaffold/isolation, `favn_legacy` is allowed to keep the active `Favn.*` namespace.
2. When a bounded slice is migrated, ownership of that `Favn.*` namespace must move to the new owner app.
3. A migrated module must not continue compiling from both `favn_legacy` and the new owner app at the same time.
4. Migration PRs must make the ownership transfer explicit in docs/tests so later slices do not reintroduce duplicate namespace providers.

### Legacy deletion criteria

Legacy code may be deleted only when all relevant criteria below are true.

Per module or slice:

1. A clearly named owner app exists in the umbrella.
2. The replacement code covers the supported behavior of the legacy slice.
3. Tests for that behavior live in the new owner app and pass there.
4. Public docs, examples, and typespec references point to the new source of truth.
5. No supported runtime path still loads or dispatches through the legacy implementation.

For larger subsystems and `favn_legacy` itself:

1. CI no longer needs the legacy code for compile, test, or runtime execution.
2. `mix favn.dev`, packaging flows, and supported deployment modes run through the new architecture.
3. Orchestrator, runner, storage, and view boundaries are exercised without the legacy runtime path.
4. `favn_legacy` is at most an optional regression reference and not a required dependency of any supported flow.
5. The repository docs describe the new architecture as the only active product shape.

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
users should not need to understand or depend directly on internal product boundaries. The public authoring contract is `favn`; runtime apps remain internal product artifacts even when they are packaged into runnable images.

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
   - the runner should stay lightweight and focused on business-code execution

3. **View is internet-facing, orchestrator and runners are private by default**
   - avoid collapsing back into one exposed runtime

4. **One user dependency**
   - user business projects should depend on `favn`, not on internal apps directly

5. **Orchestrator is the control-plane center**
   - operator APIs, run state, scheduling, manifests, and persistence belong there

6. **Minimal app count**
   - split only where there is a real deployment, dependency, or ownership boundary

7. **Legacy monolith is reference-only during migration**
   - no major new feature work there
   - use it to preserve behavior and compare outputs

## Phase roadmap

---

## Phase 0 — Freeze and reframe

### Goal
Stop feature drift on the old architecture and establish the new direction.

### Deliverables
- [x] mark old v0.5 roadmap as replaced
- [x] add this roadmap to the repo
- [x] define umbrella app list
- [x] define allowed dependency directions between apps
- [x] define migration rules
- [x] define deletion criteria for legacy code

### Exit criteria
- [x] app list is locked
- [x] dependency rules are locked
- [x] refactor is the official v0.5 direction

---

## Phase 1 — Create the umbrella and isolate legacy

### Goal
Create a safe structure for AI-assisted migration without refactoring in place.

### Deliverables
- [x] create umbrella root
- [x] scaffold:
  - [x] `favn`
  - [x] `favn_core`
  - [x] `favn_runner`
  - [x] `favn_orchestrator`
  - [x] `favn_view`
  - [x] `favn_storage_postgres`
  - [x] `favn_storage_sqlite`
  - [x] `favn_duckdb`
  - [x] `favn_test_support`
  - [x] `favn_legacy`
- [x] move current monolith code into `favn_legacy`
- [x] treat `favn_legacy` as read-only reference

### Exit criteria
- [x] umbrella compiles
- [x] legacy app remains runnable as reference
- [x] no new work happens in root `/lib` and `/test`

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
- shared cross-app migration fixtures in `favn_test_support` where needed to verify moved slices

`favn` becomes the public facade package that user projects depend on.

Transitional layout note:

- Phase 2 first guarantees namespace ownership and public API placement in `favn`
- some compiler/manifest/planning implementation may temporarily live under `favn` during this ownership transfer
- this is not the intended final architecture; after Phase 2 stabilization, internal-only implementation should be re-thinned back into `favn_core` while keeping `favn` as a thin public surface

### Exit criteria
- user business code can compile against `favn`
- manifest can be generated from user modules without orchestrator/runtime coupling
- unit tests for DSL/compiler/domain run under the new apps
- any cross-app fixtures required by migrated tests are available through `favn_test_support` test-only dependencies

---

## Phase 3 — Define and implement manifest versioning

### Goal
Make manifest versioning the source of truth for orchestration.

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md`
- `docs/refactor/PHASE_3_TODO.md`

### Deliverables
- canonical manifest schema
- version identity strategy
- manifest persistence contract for orchestrator
- run pinning to manifest version
- compatibility checks between manifest and runner runtime

Phase 3 implementation rule:

- keep `favn` as the public DSL/facade package
- move or re-center internal compiler / manifest / graph / shared-contract machinery into `favn_core`
- define a canonical serialized manifest payload separate from compile/build diagnostics and timestamps
- lock manifest hashing and compatibility rules before building runner/orchestrator runtime behavior

### Exit criteria
- a run can be created against a specific manifest version
- updating manifests does not affect in-flight runs
- orchestrator can reason about manifests without loading user business modules directly

---

## Phase 4 — Build the runner boundary

### Goal
Separate execution from orchestration.

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_4_RUNNER_BOUNDARY_PLAN.md`
- `docs/refactor/PHASE_4_TODO.md`

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

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_5_ORCHESTRATOR_BOUNDARY_PLAN.md`
- `docs/refactor/PHASE_5_TODO.md`

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

### Status
Implemented.

Phase 5 cleanup completed after initial implementation:

- collapsed the duplicated low-level storage behaviours `Favn.Storage.Adapter` and `FavnOrchestrator.Storage.Adapter` into the single authoritative `Favn.Storage.Adapter` contract to avoid future drift before Phase 6 plugin extraction

---

## Phase 6 — Add storage adapters

### Goal
Support realistic persistence modes without polluting core apps.

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_6_STORAGE_ADAPTER_PLAN.md`
- `docs/refactor/PHASE_6_TODO.md`

### Deliverables
- `favn_storage_sqlite`
- `favn_storage_postgres`

Postgres should carry forward the production-oriented transactional/persistence model already being designed, but applied to the new orchestrator architecture rather than the old monolith. :contentReference[oaicite:7]{index=7}

Phase 6 should begin with a small storage-boundary cleanup inherited from Phase 5 so the extracted adapter apps can own lifecycle and shared serialization semantics cleanly.

The remaining SQL payload / manifest-pinned SQL execution work and the final temporary seam removal in `favn` should move with Phase 7, where DuckDB/plugin extraction and runner-side SQL ownership are already the main concern.

### Exit criteria
- `mix favn.dev` works with memory by default
- `mix favn.dev --sqlite` works with persistent local state
- orchestrator can run with Postgres adapter for production-oriented setups

### Status
Implemented.

Phase 6 closed with the extracted storage foundations complete, shared adapter contract coverage in place, and atomic SQL-side storage invariants enforced for the current contract.

The remaining storage-related follow-ups were intentionally moved out of Phase 6 because they belong to later phases:

- Phase 9: local-dev `mix favn.dev --sqlite` polish and broader live Postgres verification flows
- Phase 10: adapter module rename cleanup, temporary term-blob replacement, scheduler blind-write policy decision, and external Postgres readiness optimization during final cutover

---

## Phase 7 — Move DuckDB into a runner plugin

### Goal
Prove the optional execution plugin model.

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_7_DUCKDB_RUNNER_PLAN.md`
- `docs/refactor/PHASE_7_TODO.md`

### Deliverables
- `favn_duckdb` app
- DuckDB runtime execution through runner plugin integration
- minimal DuckDB runtime placement support in the plugin path: `:in_process | :separate_process`
- plugin loading/configuration path through `favn`
- initial public plugin packaging strategy

Phase 7 should also absorb the remaining older carried-forward SQL follow-ups:

- carry SQL asset execution payload in the manifest/core contract
- enable manifest-pinned SQL asset execution in `favn_runner`
- remove temporary migration/runtime seams in `favn` once the runner SQL path is fully manifest-backed

### Exit criteria
- DuckDB is no longer mixed into the core/orchestrator boundary
- a user can depend on `favn` + `favn_duckdb`
- runner executes SQL/DuckDB functionality through the plugin path
- DuckDB placement remains runner/plugin runtime config, not manifest schema

---

## Phase 8 — Add view runtime

### Goal
Create the UI as a true external boundary.

Detailed implementation planning for this phase lives in:

- `docs/refactor/PHASE_8_VIEW_PROTOTYPE_PLAN.md`
- `docs/refactor/PHASE_8_TODO.md`

### Deliverables
In `favn_view`:

- Phoenix/LiveView app
- API integration with orchestrator
- manifest/run inspection UI
- local dev integration with `mix favn.dev`

### Exit criteria
- view runs without requiring user projects to add it as a normal Mix dependency
- view interacts with orchestrator APIs, not internal module calls into user code
- local dev can inspect compiled manifests and runs in the UI

### Status
Implemented.

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

Additional storage follow-ups in this phase:

- finish local-dev integration and polish for the extracted SQLite adapter path behind `mix favn.dev --sqlite`
- broaden live Postgres migration/transaction/concurrency verification in a production-like test path

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

Final storage cleanup in this phase:

- rename temporary extracted adapter modules back to preserved `Favn.Storage.Adapter.*` names once legacy collisions are gone
- replace temporary BEAM term-blob payload storage with the intended canonical inspectable payload format
- decide whether scheduler writes without explicit versions should remain permissive or move to stricter optimistic semantics
- replace repeated external Postgres schema-readiness checks with a clearer startup/cached readiness strategy

### Exit criteria
- all supported flows run on the new architecture
- docs describe the new product shape
- legacy app is no longer required for build/test/runtime

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
