# Manifest-First Architecture

Reader: Favn contributors and documentation agents.

Documentation type: explanation.

Favn is manifest-first. A manifest is the saved description of what the user
authored: assets, pipelines, schedules, dependencies, schema version, runner
contract version, and the metadata needed at runtime.

The manifest is the handoff between authoring and runtime. User code creates a
manifest. Runtime systems use a persisted manifest version. Runtime systems do
not rediscover user modules to decide what already accepted work means.

## Problem

Favn has separate jobs:

- `favn` gives users the public DSL and package entrypoints.
- `favn_core` owns shared manifest, compiler, graph, and validation contracts.
- `favn_orchestrator` owns persisted runtime state.
- `favn_runner` executes work.
- `favn_view` shows operator state and sends operator actions.
- Plugins and adapters connect to external systems.

If runtime code rebuilt facts from loaded modules, current source files, or
compiler helpers, then a run could mean different things after a deploy. It
would also make app ownership unclear.

## Decision

Favn uses this rule:

```text
Authoring code produces a manifest.
The orchestrator persists and activates manifest versions.
Runs use one pinned manifest version.
The runner executes work derived from that pinned version.
```

The manifest is data. It is not a scheduler, database, storage adapter, UI model,
or runner process.

## Ownership

`favn` owns the public user-facing authoring surface. Public manifest functions
belong there when users need to generate, serialize, hash, validate, or pin a
manifest through the supported package boundary.

`favn_core` owns shared manifest structs, graph logic, and validation rules used
by more than one app. Do not document its internals as public user API by
accident.

`favn_orchestrator` owns runtime control: manifest registration, persisted
manifest versions, active manifest selection, schedules, run admission,
cancellation, backfills, diagnostics, and operator-visible runtime state.

`favn_runner` owns execution. It receives pinned work that names the manifest
version and content hash. It does not choose the active manifest or own
schedules.

`favn_view` owns browser-facing UI. It must ask the orchestrator for data and
send commands through orchestrator-facing functions. It must not call storage,
runner modules, scheduler internals, repositories, compiler internals, plugins,
or adapters directly.

Plugins and adapters own external integration details. They do not own Favn run
lifecycle, schedule policy, or operator state.

## Data Flow

The safe path is:

```text
User module
  -> public Favn DSL declarations
  -> manifest data
  -> persisted manifest version in the orchestrator
  -> active manifest selection or explicit manifest version selection
  -> orchestrator run planning and admission
  -> pinned runner work
  -> runner result
  -> orchestrator run state
  -> operator UI, API, or CLI response
```

The unsafe shortcut is:

```text
Runtime process
  -> discover currently loaded user modules
  -> call compiler internals
  -> infer a current graph
```

That shortcut is rejected. Accepted runtime work must stay tied to the manifest
version that was registered when the work entered the orchestrator.

## Manifest Versions

A manifest version is an immutable wrapper around manifest data. It records the
manifest version id, content hash, schema version, runner contract version,
serialization format, and manifest payload.

The orchestrator persists manifest versions and chooses which one is active. A
run should refer to the manifest version id and content hash, not to the latest
loaded module state.

Pinning protects repeatability. If the active manifest changes later, existing
runs still point at the version they were created from.

## Failure Modes

Manifest generation can fail when authoring modules are missing, are not valid
Favn modules, declare invalid assets, reference missing dependencies, or create a
cycle.

Compatibility checks can fail when the schema version or runner contract version
is missing or unsupported.

Serialization can fail when manifest data cannot be encoded into the canonical
format.

Pinning can fail when the version id, content hash, schema version, runner
contract version, or serialization format does not match the manifest.

Registration can fail when validation rejects the manifest, storage is
unavailable, identity conflicts with existing content, or the orchestrator is not
ready.

Execution can fail, time out, or be cancelled after the run is accepted. The
runner reports what happened during execution. The orchestrator records the run
lifecycle.

## Contributor Rules

- Keep user docs focused on `favn` and the supported DSL surface.
- Keep shared manifest contracts in `favn_core`; do not move runtime ownership
  there for convenience.
- Keep manifest registration, activation, runs, schedules, cancellation,
  backfills, and diagnostics behind orchestrator-owned boundaries.
- Keep runner work pinned to a manifest version and content hash.
- Add runtime-needed facts to the manifest deliberately. Do not hide behavior in
  unvalidated metadata or private helper calls.
- Keep `favn_view` thin. If the UI needs more runtime state, add or extend an
  orchestrator-owned function instead of reading storage or runner internals.

See [`docs/structure/favn_core.md`](../structure/favn_core.md),
[`docs/structure/favn_orchestrator.md`](../structure/favn_orchestrator.md),
[`docs/structure/favn_runner.md`](../structure/favn_runner.md), and
[`docs/structure/favn_view.md`](../structure/favn_view.md) for current ownership
maps.
