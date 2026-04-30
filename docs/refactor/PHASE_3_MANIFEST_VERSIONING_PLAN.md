# Phase 3 Manifest Versioning Plan

## Status

Planned on branch `feature/phase-3-manifest-versioning-plan`.

Phase 2 established the public `Favn.*` authoring surface under `apps/favn/lib`, compile-time DSL usage through `favn`, an initial manifest generation foundation, and deterministic `{:error, :runtime_not_available}` behavior for not-yet-migrated runtime SQL paths.

That current layout is intentionally transitional. Phase 3 is the point where manifest ownership, manifest version identity, serialization, and shared runtime contracts become explicit and are re-centered into `favn_core`.

## Recommendation Summary

Phase 3 should make three architectural moves at the same time:

1. Keep `favn` as the thin public authoring package.
2. Move canonical manifest, compiler, graph, and shared runtime-contract code into `favn_core` while preserving `Favn.*` module names.
3. Define one deterministic, serialized, hashable runtime manifest contract that orchestrator and runner can both consume without loading user business modules directly.

The most important recommendation is to split the current concept of “manifest” into three distinct layers:

| Layer | Purpose | Persisted | Hashed | Owner |
| --- | --- | --- | --- | --- |
| `Favn.Manifest.Build` | compile/build result with diagnostics and authoring-only metadata | no | no | `favn_core` |
| `Favn.Manifest` | canonical runtime-required manifest payload | yes | yes | `favn_core` |
| `Favn.Manifest.Version` | immutable persisted envelope with id, hash, schema, and compatibility metadata | yes | partially | `favn_core` contract, orchestrator persists |

This split keeps timestamps, diagnostics, file paths, and other unstable compiler details out of the runtime contract and out of the manifest hash.

## Phase 3 Architecture Decisions

### What a manifest is

A manifest is the canonical, deterministic runtime contract compiled from user-authored business modules.

It is not:

- a bag of compiler diagnostics
- a snapshot of raw DSL AST
- a runtime registry cache
- a loaded-module lookup table
- an orchestrator database schema

It is:

- the normalized asset, pipeline, schedule, dependency, and execution descriptor data required by later runtimes
- sorted and serialized deterministically
- portable between authoring, orchestrator, and runner boundaries

Recommended rule:

- `Favn.Manifest` must contain only data that a persisted orchestrator record or a pinned runner work item is allowed to depend on.

### What a pinned manifest version is

A pinned manifest version is an immutable persisted record that freezes one exact canonical manifest payload for future runs.

Runs must pin to the manifest version record, not to “latest compiled manifest”.

Recommended shape:

- `manifest_version_id`: opaque orchestrator-assigned identifier, recommended ULID/UUID
- `content_hash`: deterministic hash of canonical serialized manifest bytes
- `schema_version`: manifest schema compatibility integer
- `runner_contract_version`: shared runner/orchestrator contract compatibility integer
- `serialization_format`: canonical format identifier such as `json-v1`
- `manifest`: decoded canonical manifest struct or canonical bytes depending on boundary
- optional persistence metadata such as `inserted_at`

### Version identity options

There are three valid identity strategies.

#### Option A: Hash only

Pros:

- globally content-addressable
- natural dedup key
- integrity check built in

Cons:

- awkward primary key for storage APIs
- poor ergonomic reference for later foreign keys and operator APIs
- harder to rotate hashing algorithms later

#### Option B: Opaque id only

Pros:

- simplest storage API
- good foreign-key ergonomics

Cons:

- no built-in dedup or integrity check
- no easy cross-system equality check
- difficult to verify the runner received the intended payload

#### Option C: Both id and hash

Pros:

- clean operational primary key
- content-based dedup and integrity verification
- works well for split deployment and runner verification

Cons:

- slightly more metadata to maintain

### Recommendation

Use both.

Recommended rule:

- orchestrator owns `manifest_version_id`
- `Favn.Manifest.Identity` owns `content_hash`
- runs pin by `manifest_version_id`
- runner verifies `content_hash` before execution when the hash is present in the work request

### Serialization options

There are three realistic serialization options.

#### Option A: `:erlang.term_to_binary`

Pros:

- easy inside Elixir

Cons:

- not a good product boundary
- BEAM-specific
- poor long-term compatibility and inspectability

#### Option B: ad hoc JSON per consumer

Pros:

- portable

Cons:

- easy to accidentally make non-deterministic
- encourages drift between authoring, orchestrator, and runner

#### Option C: canonical JSON serializer from one core module

Pros:

- portable and inspectable
- deterministic hashing possible
- one source of truth for persistence and transport

Cons:

- requires deliberate normalization rules up front

### Recommendation

Keep in-memory structs in Elixir, but define one canonical JSON serializer in `favn_core`.

Recommended rule:

- `content_hash = sha256(canonical_json(manifest))`
- sorted lists and sorted map keys are part of the contract
- no timestamps, diagnostics, file paths, or line numbers in the hashed payload

### Compile-time-only vs runtime-required data

Phase 3 should draw a hard line between compiler/build metadata and runtime contracts.

#### Runtime-required and allowed in `Favn.Manifest`

- canonical asset refs
- asset type and execution descriptor
- dependency graph and deterministic ordering
- window specs and schedule specs needed for future orchestration
- relation ownership and relation inputs
- SQL template IR and materialization metadata required by runner execution
- pipeline target definitions and selection data needed by control-plane planning
- connection definition references, but not resolved runtime credentials

#### Compile-time-only and not allowed in canonical runtime manifest hash

- `generated_at`
- compiler diagnostics and warnings
- source file paths and line numbers
- raw authored DSL attribute collections
- intermediate catalog caches
- `Application.get_env/3` lookup results that are not part of explicit authored contract
- loaded-module assumptions or registry state
- authoring docs used only for local developer help

#### Allowed as build metadata but not runtime contract

- diagnostics for compile feedback
- authored docs and source locations for local inspection
- compiler version and build environment data
- generation timestamps

#### `%Favn.Asset{}` field classification for Phase 3

| `%Favn.Asset{}` field | Classification | Notes |
| --- | --- | --- |
| `ref`, `module`, `name`, `type` | runtime-required | Asset identity and execution target metadata. |
| `entrypoint`, `arity` | runtime-required | Needed by runner dispatch. |
| `depends_on` | runtime-required | Canonical graph input. |
| `config` | runtime-required | Explicit authored runtime config. |
| `window_spec`, `relation`, `materialization`, `relation_inputs` | runtime-required | Required for planning/orchestration/SQL execution semantics. |
| `meta` | runtime-safe metadata | Kept when explicitly authored and contract-safe. |
| `title`, `doc` | build-only | Authoring-facing docs/labels, not required for execution compatibility. |
| `file`, `line` | build-only | Source locations only. |
| `dependencies` | build-only intermediate | Compiler-enriched dependency provenance, not canonical persisted contract. |
| `diagnostics` | build-only | Compilation feedback only. |

#### `%Favn.Pipeline.Definition{}` field classification for Phase 3

| `%Favn.Pipeline.Definition{}` field | Classification | Notes |
| --- | --- | --- |
| `module`, `name` | runtime-required | Pipeline identity and ownership. |
| `selectors`, `deps` | runtime-required | Deterministic target resolution behavior. |
| `schedule`, `window`, `source`, `outputs` | runtime-required | Control-plane run creation semantics. |
| `config` | runtime-required | Explicit runtime pipeline configuration. |
| `meta` | runtime-safe metadata | Allowed if authored and contract-safe. |
| `selection_mode` | build-only | Authoring-mode detail; canonical persisted selectors are already explicit. |

### What orchestrator and runner are allowed to depend on

#### Orchestrator may depend on

- persisted `Favn.Manifest.Version`
- canonical manifest bytes or decoded canonical `Favn.Manifest`
- asset refs, dependency graph, pipeline descriptors, schedule descriptors, and compatibility metadata
- manifest hash and compatibility validation results

#### Runner may depend on

- pinned manifest version metadata
- manifest execution descriptors for the assets it is asked to run
- module names and entrypoints encoded in the manifest
- SQL IR/materialization metadata encoded in the manifest
- runner work request structs defined in `favn_core`

#### Orchestrator and runner must not depend on

- direct runtime module discovery from user business projects
- `Favn.list_assets/0` over loaded modules at runtime
- compiler diagnostics
- file paths, line numbers, or raw DSL metadata
- transient build-time registries or `:persistent_term` compiler caches

### Compatibility expectations between manifest version and runner runtime

Phase 3 should choose strict compatibility first.

Recommended rule for v0.5:

- `schema_version` mismatch is a hard error
- `runner_contract_version` mismatch is a hard error
- compatibility ranges are out of scope until there is a real operational need

Recommended validation points:

- authoring/build time validates the manifest can be serialized canonically
- orchestrator persistence validates manifest schema/contract versions before accepting a new record
- runner validates schema version, contract version, and content hash before starting work

### How this supports the future three-deployment architecture

This Phase 3 contract directly supports the intended product split:

- `favn` compiles authoring modules into canonical manifest data
- `favn_orchestrator` persists immutable manifest versions and schedules runs from persisted manifests only
- `favn_runner` executes pinned manifest-backed work without asking the orchestrator to load user code directly
- `favn_view` reads orchestrator projections and never depends on business modules or compiler internals

## Canonical Phase 3 Schema Recommendation

Recommended canonical runtime payload fields for `Favn.Manifest`:

| Field | Notes |
| --- | --- |
| `schema_version` | integer for manifest structure compatibility |
| `runner_contract_version` | integer for runner/orchestrator shared contract compatibility |
| `assets` | sorted asset manifest entries |
| `pipelines` | sorted pipeline manifest entries |
| `schedules` | sorted schedule manifest entries |
| `graph` | normalized dependency graph / topo data |
| `metadata` | runtime-safe metadata only; no diagnostics/timestamps/source locations |

Recommended supporting structs:

| Struct | Purpose |
| --- | --- |
| `Favn.Manifest.Asset` | canonical asset descriptor, including execution descriptor |
| `Favn.Manifest.Pipeline` | canonical pipeline descriptor for later run creation |
| `Favn.Manifest.Schedule` | canonical persisted schedule descriptor |
| `Favn.Manifest.Graph` | normalized dependency edges and deterministic topo order |
| `Favn.Manifest.Build` | compile/build wrapper around the canonical manifest plus diagnostics |
| `Favn.Manifest.Version` | immutable persisted manifest-version envelope |

Recommended explicit exclusions from canonical payload:

- `generated_at`
- diagnostics
- raw `%Favn.Asset{}` copies embedded wholesale
- `%Favn.Pipeline.Definition{}` copies embedded wholesale when they include authoring-only fields

## Public vs Internal Ownership

Phase 3 should make the ownership call explicitly:

- `favn` owns public DSL and public facade only
- `favn_core` owns compiler, manifest, planning helpers, graph logic, and shared contracts

Important implementation note:

- module names may remain `Favn.*` even when files move physically into `apps/favn_core/lib/favn/...`
- Phase 3 is a physical owner-app move, not a namespace rename

### Files that should remain public in `favn`

| Current file | Phase 3 classification | Why |
| --- | --- | --- |
| `apps/favn/lib/favn.ex` | copy and edit | Keep as the thin public facade; delegate to `favn_core` for build, serialization, versioning, and compatibility work; stop growing runtime-oriented seams here. |
| `apps/favn/lib/favn/asset.ex` | copy and edit | Public authoring DSL stays in `favn`; imports and aliases should point at core-owned structs/helpers. |
| `apps/favn/lib/favn/assets.ex` | copy and edit | Same reason as `Favn.Asset`; public multi-definition DSL surface stays public. |
| `apps/favn/lib/favn/multi_asset.ex` | copy and edit | Public DSL convenience surface. |
| `apps/favn/lib/favn/source.ex` | copy and edit | Public source-authoring DSL. |
| `apps/favn/lib/favn/pipeline.ex` | copy and edit | Public pipeline DSL remains user-facing; compiled structs it emits should be core-owned. |
| `apps/favn/lib/favn/sql_asset.ex` | copy and edit | Public SQL asset DSL remains in `favn`; internal IR and manifest output belong in `favn_core`. |
| `apps/favn/lib/favn/sql.ex` | copy and edit | Keep only the public SQL authoring DSL and helper macros here; do not let runtime contracts or manifest internals settle in this file. |
| `apps/favn/lib/favn/namespace.ex` | copy and edit | Public namespace DSL should remain user-facing. |
| `apps/favn/lib/favn/window.ex` | copy and edit | Public window authoring helpers remain public while core owns window structs/validators. |
| `apps/favn/lib/favn/triggers/schedules.ex` | copy and edit | Public schedule DSL remains in `favn`; emitted schedule structs should be core-owned. |
| `apps/favn/lib/favn/connection.ex` | copy and edit | Public behaviour and authoring-facing connection declaration remain public; runtime loading/registry work stays out. |
| `apps/favn/lib/favn/public_scaffold.ex` | move later as cleanup, but not required for initial Phase 3 implementation | Temporary status marker only; it should not drive final architecture. |

### Files that should move or re-center into `favn_core`

| Current file | New owner path | Phase 3 classification | Why |
| --- | --- | --- | --- |
| `apps/favn/lib/favn/ref.ex` | `apps/favn_core/lib/favn/ref.ex` | copy as-is | Pure canonical identity value object. |
| `apps/favn/lib/favn/relation_ref.ex` | `apps/favn_core/lib/favn/relation_ref.ex` | copy as-is | Pure relation identity contract shared by compiler/orchestrator/runner. |
| `apps/favn/lib/favn/timezone.ex` | `apps/favn_core/lib/favn/timezone.ex` | copy as-is | Pure validation helper, not a public DSL concern. |
| `apps/favn/lib/favn/diagnostic.ex` | `apps/favn_core/lib/favn/diagnostic.ex` | copy and edit | Diagnostics are compiler/build output, not public DSL ownership. |
| `apps/favn/lib/favn/dsl/compiler.ex` | `apps/favn_core/lib/favn/dsl/compiler.ex` | copy as-is | Shared compile-time helper. |
| `apps/favn/lib/favn/asset/dependency.ex` | `apps/favn_core/lib/favn/asset/dependency.ex` | copy as-is | Pure dependency metadata contract. |
| `apps/favn/lib/favn/asset/relation_input.ex` | `apps/favn_core/lib/favn/asset/relation_input.ex` | copy as-is | Pure typed relation-input contract. |
| `apps/favn/lib/favn/asset/relation_resolver.ex` | `apps/favn_core/lib/favn/asset/relation_resolver.ex` | copy and edit | Pure normalization logic, but must align to canonical manifest fields and runtime-safe relation data. |
| `apps/favn/lib/favn/window/spec.ex` | `apps/favn_core/lib/favn/window/spec.ex` | copy as-is | Pure domain struct/validation. |
| `apps/favn/lib/favn/window/anchor.ex` | `apps/favn_core/lib/favn/window/anchor.ex` | copy as-is | Pure window anchor domain. |
| `apps/favn/lib/favn/window/runtime.ex` | `apps/favn_core/lib/favn/window/runtime.ex` | copy as-is | Shared runtime window value object. |
| `apps/favn/lib/favn/window/key.ex` | `apps/favn_core/lib/favn/window/key.ex` | copy as-is | Shared identity encoding/decoding. |
| `apps/favn/lib/favn/window/validate.ex` | `apps/favn_core/lib/favn/window/validate.ex` | copy as-is | Shared validation helper. |
| `apps/favn/lib/favn/triggers/schedule.ex` | `apps/favn_core/lib/favn/triggers/schedule.ex` | copy and edit | Shared schedule struct/validation belongs in core; remove ambient config assumptions from this layer. |
| `apps/favn/lib/favn/connection/definition.ex` | `apps/favn_core/lib/favn/connection/definition.ex` | copy as-is | Static connection-definition contract. |
| `apps/favn/lib/favn/pipeline/definition.ex` | `apps/favn_core/lib/favn/pipeline/definition.ex` | copy and edit | Canonical pipeline definition struct is internal shared contract, not DSL ownership. |
| `apps/favn/lib/favn/assets/compiler.ex` | `apps/favn_core/lib/favn/assets/compiler.ex` | copy and edit | Compiler seam belongs in core and should stop resolving through public facade state. |
| `apps/favn/lib/favn/assets/dependency_inference.ex` | `apps/favn_core/lib/favn/assets/dependency_inference.ex` | copy and edit | Pure compile-time logic shared across authoring/runtime contracts. |
| `apps/favn/lib/favn/assets/graph_index.ex` | `apps/favn_core/lib/favn/assets/graph.ex` or `apps/favn_core/lib/favn/manifest/graph.ex` | rewrite from scratch | Current file is a global cached graph over `Application.get_env/3` and `:persistent_term`; Phase 3 needs a pure deterministic graph builder. |
| `apps/favn/lib/favn/manifest.ex` | `apps/favn_core/lib/favn/manifest.ex` | rewrite from scratch | Current struct mixes timestamped build output with runtime contract concerns. |
| `apps/favn/lib/favn/manifest/generator.ex` | `apps/favn_core/lib/favn/manifest/generator.ex` | rewrite from scratch | Phase 3 needs deterministic manifest/build/version separation, serializer integration, and stable ordering rules. |
| `apps/favn/lib/favn/manifest/asset.ex` | `apps/favn_core/lib/favn/manifest/asset.ex` | rewrite from scratch | Current entry wraps the entire `%Favn.Asset{}`; Phase 3 needs an explicit runtime descriptor shape instead. |
| `apps/favn/lib/favn/manifest/pipeline.ex` | `apps/favn_core/lib/favn/manifest/pipeline.ex` | rewrite from scratch | Current entry embeds code-level definitions too directly; Phase 3 needs persisted pipeline descriptors. |
| `apps/favn/lib/favn/manifest/schedule.ex` | `apps/favn_core/lib/favn/manifest/schedule.ex` | copy and edit | Struct idea is valid, but Phase 3 must align it to persisted schedule descriptors and serializer rules. |
| `apps/favn/lib/favn/manifest/catalog.ex` | `apps/favn_core/lib/favn/manifest/catalog.ex` | copy and edit | Useful as an intermediate compiler shape, but should not be confused with persisted manifest schema. |
| `apps/favn/lib/favn/sql/definition.ex` | `apps/favn_core/lib/favn/sql/definition.ex` | copy as-is | Pure SQL definition IR. |
| `apps/favn/lib/favn/sql/template.ex` | `apps/favn_core/lib/favn/sql/template.ex` | copy and edit | Pure compile-time SQL IR/parser; Phase 3 must ensure it serializes cleanly into manifest runtime descriptors. |
| `apps/favn/lib/favn/sql/source.ex` | `apps/favn_core/lib/favn/sql/source.ex` | copy as-is | Compile-time SQL file source abstraction. |
| `apps/favn/lib/favn/sql_asset/definition.ex` | `apps/favn_core/lib/favn/sql_asset/definition.ex` | copy and edit | Shared SQL asset intermediate definition belongs in core. |
| `apps/favn/lib/favn/sql_asset/compiler.ex` | `apps/favn_core/lib/favn/sql_asset/compiler.ex` | copy and edit | Compiler output must feed the canonical manifest builder. |
| `apps/favn/lib/favn/sql_asset/materialization.ex` | `apps/favn_core/lib/favn/sql_asset/materialization.ex` | copy as-is | Pure materialization metadata contract. |
| `apps/favn/lib/favn/sql_asset/relation_usage.ex` | `apps/favn_core/lib/favn/sql_asset/relation_usage.ex` | copy and edit | Pure relation-usage collector, but it should produce manifest-safe runtime descriptors. |

### Files that should not be touched until Phase 4 or Phase 5

| Current file | Phase 3 classification | Why |
| --- | --- | --- |
| `apps/favn/lib/favn/plan.ex` | moved in Phase 3 to `apps/favn_core/lib/favn/plan.ex` | Ownership was re-centered to keep planning contracts in `favn_core` while preserving module names. |
| `apps/favn/lib/favn/assets/planner.ex` | moved in Phase 3 to `apps/favn_core/lib/favn/assets/planner.ex` | Planner internals now live in `favn_core` and are consumed through the public facade. |
| `apps/favn/lib/favn/pipeline/resolver.ex` | moved in Phase 3 to `apps/favn_core/lib/favn/pipeline/resolver.ex` | Pipeline runtime-handoff shaping is now core-owned to avoid split ownership. |
| `apps/favn/lib/favn/pipeline/resolution.ex` | moved in Phase 3 to `apps/favn_core/lib/favn/pipeline/resolution.ex` | Resolution contract ownership is now `favn_core`. |
| `apps/favn_legacy/lib/favn/runtime/**` | do not touch in Phase 3 | Full runner/runtime migration is Phase 4 work. |
| `apps/favn_legacy/lib/favn/run/**` | do not touch in Phase 3 | Run lifecycle ownership moves in Phase 5. |
| `apps/favn_legacy/lib/favn/scheduler/**` | do not touch in Phase 3 | Orchestrator scheduling is Phase 5. |
| `apps/favn_legacy/lib/favn/storage/**` | do not touch in Phase 3 | Storage adapters are Phase 6. |
| `apps/favn_legacy/lib/favn/sql/render.ex` | do not touch in Phase 3 | SQL execution/rendering runtime ownership moves with runner/plugin work, not manifest versioning. |
| `apps/favn_legacy/lib/favn/sql/session.ex` | do not touch in Phase 3 | Runtime connection/session work is not a manifest concern. |
| `apps/favn_legacy/lib/favn/sql/adapter/**` | do not touch in Phase 3 | Plugin/runtime work belongs in later phases. |
| `apps/favn_legacy/lib/favn/sql_asset/runtime.ex` | do not touch in Phase 3 | Full SQL runtime ownership migration is out of scope. |

## New Files To Create In Phase 3

The following new files/modules should be introduced in `favn_core` during Phase 3.

| New file | Owner app | Purpose | Why Phase 3 |
| --- | --- | --- | --- |
| `apps/favn_core/lib/favn/manifest/build.ex` | `favn_core` | Compiler/build result wrapper containing the canonical manifest plus diagnostics/build metadata | Needed now to separate build-time concerns from runtime contracts and hashing. |
| `apps/favn_core/lib/favn/manifest/version.ex` | `favn_core` | Immutable persisted manifest-version envelope with id/hash/schema/compatibility fields | Runs must pin to this before runner/orchestrator implementation starts. |
| `apps/favn_core/lib/favn/manifest/identity.ex` | `favn_core` | Canonical hash generation and identity helpers | Needed now to define stable content addressing and dedup/integrity behavior. |
| `apps/favn_core/lib/favn/manifest/serializer.ex` | `favn_core` | Canonical serializer/deserializer for persisted and transported manifest data | Needed now to lock deterministic bytes and cross-app transport format. |
| `apps/favn_core/lib/favn/manifest/compatibility.ex` | `favn_core` | Manifest schema and runner-contract compatibility validation | Needed now so later runner/orchestrator work targets a frozen compatibility contract. |
| `apps/favn_core/lib/favn/manifest/graph.ex` | `favn_core` | Pure normalized dependency graph embedded in canonical manifest | Needed now to remove global graph caches and define persisted graph shape. |
| `apps/favn_core/lib/favn/contracts/runner_work.ex` | `favn_core` | Shared manifest-backed runner work request struct | Needed now because Phase 4 should build against a pre-defined work contract instead of inventing one ad hoc. |
| `apps/favn_core/lib/favn/contracts/runner_result.ex` | `favn_core` | Shared runner result DTO | Needed now so the runner/orchestrator seam has a stable shared output shape. |
| `apps/favn_core/lib/favn/contracts/runner_event.ex` | `favn_core` | Shared execution event DTO | Needed now to keep future event transport compatible with persisted manifest versions. |

Optional but recommended if needed to keep naming cleaner:

- `apps/favn_core/lib/favn/manifest/runtime_metadata.ex`
- `apps/favn_core/lib/favn/manifest/version_ref.ex`

These should only be added if the primary structs above become overloaded.

## File-By-File Migration Strategy

Recommended Phase 3 work order by slice:

1. Create the new `favn_core` manifest/versioning/serializer/compatibility modules from scratch.
2. Move pure value objects and compile-time helpers from `favn` into `favn_core` with no behavior change.
3. Rebuild manifest generation around the new core-owned canonical schema.
4. Thin `favn` facade and DSL modules so they depend on the core contracts rather than owning them.
5. Add shared contract DTOs for future runner/orchestrator work.
6. Leave execution runtime, scheduling, and storage concerns untouched.

Recommended migration rule for moved files:

- if a file currently mixes public DSL and internal logic, keep the public DSL entrypoint in `favn` and move the internal structs/helpers into `favn_core`
- if a file is pure and reusable, copy it first with no behavioral change and delete the `favn` copy in the same slice
- if a file relies on `Application.get_env/3`, `:persistent_term`, runtime manager modules, or execution lifecycle concepts, rewrite it or defer it

## Testing Plan

Phase 3 should lock behavior before runner/orchestrator implementation begins.

### Tests to add in `apps/favn_core/test`

Add new focused test files for:

- `manifest/build_test.exs`
- `manifest/version_test.exs`
- `manifest/identity_test.exs`
- `manifest/serializer_test.exs`
- `manifest/compatibility_test.exs`
- `manifest/graph_test.exs`
- `contracts/runner_work_test.exs`
- `contracts/runner_result_test.exs`
- `contracts/runner_event_test.exs`

Behavior that must be locked in `favn_core`:

- canonical manifest lists are deterministically sorted
- canonical manifest bytes are stable for semantically identical input
- timestamps and diagnostics do not affect `content_hash`
- hash changes when runtime-significant manifest fields change
- schema version and runner contract version mismatches fail deterministically
- canonical manifest round-trips through the serializer without loss
- graph shape is pure and no longer depends on `Application.get_env/3` or `:persistent_term`
- runner work/result/event DTOs serialize cleanly and carry manifest-version identity correctly

### Tests to add or keep in `apps/favn/test`

`apps/favn/test` should shrink to public-surface tests only.

Add or keep focused tests for:

- DSL modules still compiling through `use Favn.*`
- public `Favn.generate_manifest/1` delegating to core-owned builder APIs
- public facade APIs for serialize/version/build compatibility checks
- docs/doctests for the public authoring surface

Tests that should move out of `apps/favn/test` into `apps/favn_core/test` during Phase 3:

- current manifest generator determinism tests
- graph/deterministic ordering tests
- compiler/helper tests that do not exercise the public facade itself

### What should remain in legacy for now

Keep these in `apps/favn_legacy/test` until later phases:

- runtime execution tests
- scheduler tests
- run lifecycle tests
- storage adapter tests
- SQL runtime/render/session tests
- plugin/backend tests

Phase 3 should not try to migrate those suites yet.

### Contract tests required before Phase 4 starts

The following behavior must be locked before runner work begins:

- canonical manifest schema fields are agreed and serialized deterministically
- pinned manifest version envelope and identity rules are stable
- runner compatibility validation behavior is stable
- runner work request/result/event DTO shapes are stable
- compile-time-only metadata is explicitly excluded from runtime hash/compatibility behavior
- orchestrator-safe manifest projection is usable without loading user business modules

## Documentation Updates Required In Phase 3

Phase 3 implementation should update at least:

- `README.md`
- `docs/REFACTOR.md`
- `docs/FEATURES.md`
- `docs/structure/`

Phase 3 implementation should keep the following honest:

- Phase 2 achieved public `Favn.*` ownership and initial manifest foundations
- current internal implementation still lives mostly in `favn`
- Phase 3 exists to re-center that internal ownership into `favn_core`

## Explicit Out-Of-Scope List

Do not implement these in Phase 3:

- full runner execution engine
- runner supervision/runtime boot
- orchestrator scheduling/runtime
- run lifecycle state machine migration
- storage adapters or persistence backends
- UI/view implementation
- full SQL runtime ownership migration
- DuckDB/plugin migration
- local dev tooling and packaging flows
- runtime connection/session registries
- broad compatibility-range negotiation beyond strict exact-match validation

Phase 3 is successful when manifest schema, manifest version identity, serialization, compatibility rules, and shared contracts are locked well enough that Phases 4 and 5 can build on them without redefining the boundary.
