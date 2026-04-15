# Phase 4 Runner Boundary Plan

## Status

Implemented in active slices on branch `feature/phase-4-runner-plan`; remaining Phase 4 work is cleanup and hardening.

Phase 3 locked the manifest schema, manifest version envelope, serializer/hash rules, and shared runner DTOs in `favn_core`.

`favn_runner` now owns runner server/worker execution, manifest registration/lookup, runner-side connection runtime ownership, and SQL runtime execution ownership.

Control-plane run lifecycle, scheduler, storage, and projection ownership remain in `favn_legacy` until Phase 5/6.

Phase 4 is the point where execution stops being a legacy in-process runtime concern and becomes an explicit runner-owned boundary.

## Implementation Snapshot

The current implementation aligns with the main architecture recommendations in this plan:

- runner protocol server exists (`register_manifest/1`, `submit_work/2`, `await_result/2`, `cancel_work/2`, `run/2`)
- manifest registration is pinned by version id/hash and validated before execution
- same-node execution crosses the runner boundary
- `Favn.Run.Context` and `Favn.Run.AssetResult` moved into `favn_core`
- runner-side `Favn.Connection.*` runtime modules moved into `favn_runner`
- runner-side `Favn.SQLAsset.Runtime` and required `Favn.SQL.*` runtime modules moved into `favn_runner`

Authoritative completion tracking for this phase lives in `docs/refactor/PHASE_4_TODO.md`.

## Recommendation Summary

Phase 4 should make four architectural moves together:

1. Build `favn_runner` as an execution-only OTP app around pinned manifest versions.
2. Make manifest handoff a two-step boundary: register a `%Favn.Manifest.Version{}` first, then submit `%Favn.Contracts.RunnerWork{}` that references it by id/hash.
3. Move user-visible runtime execution contracts out of `favn_legacy` before building the runner engine, especially `Favn.Run.Context` and `Favn.Run.AssetResult`.
4. Make same-node local execution use the same runner server boundary that later Phase 5 orchestrator dispatch will use.

The most important recommendation is to keep the runner boundary narrow:

- runner owns manifest lookup, execution context construction, asset invocation, runner events, and connection/runtime value resolution
- runner does not own scheduling, run lifecycle policy, retry policy, persistence, or operator projections
- same-node mode should still cross the runner protocol seam instead of calling asset modules directly from orchestrator code

## Why Phase 4 Exists Now

Phase 3 made the manifest and version envelope stable enough that execution can stop depending on runtime module discovery.

Today, the legacy runtime still does all of the following in one subsystem:

- plan orchestration and run lifecycle
- execution dispatch
- context building for user asset code
- runtime connection loading
- SQL asset execution and adapter/session ownership
- event publication and snapshot persistence

That is too much ownership for the future product shape. Phase 4 should split out only the execution half and leave control-plane behavior for Phase 5.

## Phase 4 Architecture Decisions

### 1. Runner work granularity

Phase 4 should execute one concrete plan node per work item.

Recommended rule:

- `%Favn.Contracts.RunnerWork{}` must resolve to exactly one target asset for Phase 4 execution
- `asset_ref` is the preferred target field
- `asset_refs` may remain in the DTO for future batching, but Phase 4 should reject zero-target and multi-target work requests rather than invent batch semantics now

Why:

- Phase 5 orchestrator will already own dependency planning and node readiness
- retry/cancel/state transitions belong to orchestrator, not to runner-internal mini schedulers
- single-node execution keeps the protocol simple and aligns with the current `Favn.Plan.node_key()` model

### 2. Manifest handoff and loading

The shared Phase 3 work contract intentionally carries manifest identity, not the full manifest payload.

Phase 4 should therefore use a two-step handoff:

1. register `%Favn.Manifest.Version{}` with the runner
2. submit `%Favn.Contracts.RunnerWork{}` that references `manifest_version_id` and `manifest_content_hash`

Recommended rule:

- the runner server accepts `%Favn.Manifest.Version{}` directly as the manifest registration contract
- manifest registration is idempotent by `{manifest_version_id, content_hash}`
- runner execution fails fast if work references a manifest version the runner has not registered

This avoids re-sending the full manifest payload on every work request while preserving the strict Phase 3 identity rules.

### 3. Manifest store inside `favn_runner`

`favn_runner` should own a manifest version store optimized for runtime reads, not persistence.

Recommended module split:

- `FavnRunner.ManifestStore`
  - register/fetch/delete registered manifest versions
  - key by `manifest_version_id`
  - verify `content_hash` on fetch when work includes one
- `FavnRunner.ManifestResolver`
  - validate runner compatibility with `%Favn.Manifest.Version{}`
  - resolve `%Favn.Manifest.Asset{}` for a requested `asset_ref`
  - reject unknown refs and incompatible manifest/schema versions

Recommended implementation shape for Phase 4:

- ETS or a small GenServer-backed in-memory store is enough
- persistence stays out of scope; orchestrator will own durable storage in Phase 5

### 4. Runner protocol server

`favn_runner` should expose one internal protocol server API that both same-node mode and later orchestrator dispatch can target.

Recommended API surface in `FavnRunner` / `FavnRunner.Server`:

- `register_manifest(version)`
- `submit_work(work, opts \\ [])`
- `await_result(execution_id, timeout \\ 5_000)`
- `cancel_work(execution_id, reason \\ %{})`
- `run(work, opts \\ [])` as a synchronous convenience wrapper for local/dev tests only

Recommended behavior:

- `submit_work/2` starts a supervised worker and returns an opaque runner execution id
- `await_result/2` is same-node convenience, not the long-term remote transport contract
- `cancel_work/2` only cancels active runner work; full run-level cancellation policy remains Phase 5 orchestrator logic

### 5. Supervision tree

Phase 4 should keep the supervision tree small and boring.

Recommended children under `FavnRunner.Application`:

- `FavnRunner.ManifestStore`
- runner-side connection registry/process, if a process is used
- `Registry` for active execution lookups/subscriptions
- `DynamicSupervisor` for execution workers
- `FavnRunner.Server`

Do not introduce a scheduler, planner, or persistence layer here.

### 6. Execution pipeline inside the runner

Each worker should follow one explicit pipeline:

1. resolve registered manifest version by `manifest_version_id`
2. validate `content_hash` and compatibility using `Favn.Manifest.Compatibility`
3. resolve the requested `%Favn.Manifest.Asset{}` from the manifest
4. build `%Favn.Run.Context{}` from work request + manifest asset + runtime inputs
5. dispatch by asset type
6. emit `%Favn.Contracts.RunnerEvent{}` values during execution
7. return `%Favn.Contracts.RunnerResult{}` with one `%Favn.Run.AssetResult{}` entry

Recommended asset-type dispatch behavior:

- `:source` assets: do not invoke user code; return an observed/no-op result with relation metadata
- `:elixir` assets: call `apply(module, entrypoint, [ctx])` using the manifest execution descriptor
- `:sql` assets: route through runner-owned SQL asset runtime modules, not through legacy coordinator paths

### 7. Runtime context ownership

`Favn.Run.Context` is no longer just a legacy runtime detail. User business code executes against it, so it must move out of `favn_legacy` as Phase 4 starts.

Recommendation:

- move `Favn.Run.Context` into `apps/favn_core/lib/favn/run/context.ex`
- move `Favn.Run.AssetResult` into `apps/favn_core/lib/favn/run/asset_result.ex`

Why these belong in `favn_core`:

- runner constructs them
- orchestrator will later persist/reference them
- user code and public docs refer to `Favn.*` runtime contracts, not `FavnRunner.*`

Do not keep these contracts legacy-owned once runner execution starts moving.

### 8. Connection and runtime value resolution

Connection resolution is runner-side behavior because it turns authored connection definitions into executable runtime configuration and secrets.

Recommended ownership split:

- stay in `favn_core`: `Favn.Connection.Definition`
- move to `favn_runner` under preserved `Favn.*` names: `Favn.Connection.Loader`, `Favn.Connection.Registry`, `Favn.Connection.Resolved`, `Favn.Connection.Validator`, `Favn.Connection.Error`, `Favn.Connection.Sanitizer`, `Favn.Connection.Info`

Recommended Phase 4 rule:

- manifest stores only authored connection references and static metadata
- runner resolves runtime connection values from its own config at the edge
- workers should receive resolved connection access through explicit runner-owned modules rather than doing direct `Application.get_env/3` reads in the hot path

This keeps secrets and runtime environment lookup at the execution boundary where they belong.

### 9. SQL execution migration strategy

Phase 4 needs a runner-side SQL execution path, but full plugin extraction is still Phase 7.

Recommendation:

- move the runner-owned SQL runtime path out of `favn_legacy` now
- keep the move narrow and execution-focused
- do not wait for Phase 7 to make SQL assets executable behind the runner boundary

Recommended ownership split for the Phase 4 slice:

- move pure shared value structs to `favn_core` only if both public facade and runner need them
- move runtime session/connection/asset-execution behavior to `favn_runner`
- keep adapter/plugin extraction itself deferred until `favn_duckdb` work in Phase 7

Expected legacy source modules for this slice:

- `apps/favn_legacy/lib/favn/sql_asset/runtime.ex`
- backend-neutral runtime structs currently in `apps/favn_legacy/lib/favn/sql/*.ex`
- runner-side session and adapter entrypoints required for local execution

Important rule:

- `favn_runner` must not take a dependency on `favn_legacy`
- if a SQL execution module is required by the new runner, it must be moved or rewritten in the new owner app during the slice

### 10. Event model

Runner events in Phase 4 should be transportable execution facts, not the final operator event system.

Recommended rule:

- workers emit `%Favn.Contracts.RunnerEvent{}` for execution start, success, failure, cancel, and timeout
- runner may expose local subscription/await helpers for same-node tests and dev flows
- event history persistence, projections, PubSub fanout, and operator APIs remain Phase 5 orchestrator concerns

Do not recreate the full legacy `Favn.Runtime.Events` subsystem as the permanent runner design.

### 11. Same-node local mode

Same-node mode must still cross the runner boundary.

Recommended local flow:

1. authoring side builds/pins a manifest version
2. same-node runtime registers that `%Favn.Manifest.Version{}` with `favn_runner`
3. local dispatch submits `%Favn.Contracts.RunnerWork{}` to `FavnRunner.Server`
4. result and events come back through runner contracts only

That gives Phase 5 orchestrator a real seam to target rather than another in-process shortcut to replace later.

## Legacy Slice Map

Phase 4 should pull from legacy by bounded vertical slice.

### Move or rewrite early in Phase 4

- `apps/favn_legacy/lib/favn/run/context.ex`
- `apps/favn_legacy/lib/favn/run/asset_result.ex`
- `apps/favn_legacy/lib/favn/runtime/executor.ex`
- `apps/favn_legacy/lib/favn/runtime/executor/local.ex`
- `apps/favn_legacy/lib/favn/connection/*.ex`
- `apps/favn_legacy/lib/favn/sql_asset/runtime.ex`
- the smallest backend-neutral `apps/favn_legacy/lib/favn/sql/*.ex` subset required for runner execution

### Leave in legacy for Phase 5+

- `apps/favn_legacy/lib/favn/runtime/manager.ex`
- `apps/favn_legacy/lib/favn/runtime/coordinator.ex`
- `apps/favn_legacy/lib/favn/runtime/state.ex`
- `apps/favn_legacy/lib/favn/runtime/transitions/*.ex`
- `apps/favn_legacy/lib/favn/runtime/projector.ex`
- `apps/favn_legacy/lib/favn/runtime/events.ex`
- `apps/favn_legacy/lib/favn/storage/**/*.ex`
- `apps/favn_legacy/lib/favn/scheduler/**/*.ex`

That split keeps execution in Phase 4 and control-plane persistence/state in Phase 5.

## File Plan

Recommended new or rewritten files for the first Phase 4 implementation slice:

### In `apps/favn_core/lib/favn/`

- `run/context.ex`
- `run/asset_result.ex`

### In `apps/favn_runner/lib/`

- `favn_runner.ex`
- `favn_runner/application.ex`

### In `apps/favn_runner/lib/favn_runner/`

- `server.ex`
- `manifest_store.ex`
- `manifest_resolver.ex`
- `worker.ex`
- `context_builder.ex`
- `event_sink.ex` or equivalent runner event emitter module

### In `apps/favn_runner/lib/favn/`

- moved/reowned `connection/*.ex` runtime modules
- moved/reowned SQL runtime modules required for asset execution

The preserved `Favn.*` namespace remains important even when files move physically into `favn_runner` or `favn_core`.

## Implementation Order

Recommended Phase 4 work order:

1. Move shared runtime contracts (`Favn.Run.Context`, `Favn.Run.AssetResult`) out of legacy.
2. Replace `favn_runner` scaffold with the runner server, worker supervisor, and manifest store.
3. Implement manifest registration and manifest-backed asset resolution.
4. Implement Elixir asset execution and source-asset observe/no-op behavior.
5. Move runner-side connection resolution into `favn_runner`.
6. Move the minimal SQL runtime execution slice into `favn_runner`.
7. Add same-node integration tests that register a manifest version and submit real runner work.
8. Update docs and roadmap status once legacy ownership is no longer the active path for migrated execution slices.

## Testing Plan

Phase 4 should add focused tests in `apps/favn_runner/test` for:

- manifest registration and fetch behavior
- hash/version mismatch failures
- unknown manifest version failures
- unknown asset ref failures
- Elixir asset invocation through manifest execution descriptors
- source asset observe/no-op execution
- connection resolution and validation
- SQL asset execution through the runner path
- cancellation of active work
- same-node submit/await integration through `FavnRunner.Server`

Phase 4 should add or move focused tests in `apps/favn_core/test` for:

- `Favn.Run.Context`
- `Favn.Run.AssetResult`

Legacy tests that should stay where they are for now:

- run lifecycle manager/coordinator tests
- scheduler tests
- storage adapter tests
- operator event history/projection tests

## Documentation Updates Required In Phase 4

Phase 4 implementation should update at least:

- `README.md`
- `docs/REFACTOR.md`
- `docs/FEATURES.md`
- `docs/lib_structure.md`
- `docs/test_structure.md`

The docs must make these points explicit:

- Phase 3 locked manifest/version/contract rules in `favn_core`
- Phase 4 moves execution ownership into `favn_runner`
- `favn_runner` remains execution-only and does not become a second orchestrator
- same-node mode now goes through a real runner boundary

## Explicit Out-Of-Scope List

Do not implement these as part of Phase 4:

- orchestrator run lifecycle state machine migration
- scheduling or trigger persistence
- durable manifest storage
- full operator event history/projections
- storage adapters
- view/UI work
- plugin extraction into `favn_duckdb`
- cross-version compatibility negotiation beyond the strict Phase 3 rules
- distributed remote transport polish beyond what is needed to avoid painting Phase 5 into a corner

Phase 4 is successful when execution can happen through `favn_runner` using registered pinned manifest versions, without the orchestrator or control-plane code loading user business modules directly.
