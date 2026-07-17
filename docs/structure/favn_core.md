# favn_core

Purpose: shared compiler, manifest, planning, graph, execution policy,
freshness, window, static settings, runtime config refs/bundles/field-level merge contracts, backfill range, and cross-runtime contract
types.

Code:
- `apps/favn_core/lib/favn/`
- `apps/favn_core/lib/favn/contracts/` owns shared runner-facing work, result,
  error, cancellation, logging, and inspection contract structs
- `apps/favn_core/lib/favn/plan/node_identity.ex` owns the planned-node identity
  seam consumed by runner work without exposing graph/index internals
- `apps/favn_core/lib/favn/manifest/labels.ex` owns tag/category label
  normalization so selector-facing metadata persists and matches as strings
- `apps/favn_core/lib/favn/runner/` owns the public runner plugin lifecycle and
  simple supervised-children implementation
- `apps/favn_core/lib/favn/runtime_value*` owns inert provider refs for deferred,
  explicitly supported runtime-boundary values; refs never contain resolved
  credentials

Tests:
- `apps/favn_core/test/`
- App-local tests must use only `favn_core` and declared test support; authoring
  DSL parity tests live in `favn_authoring`.
- `apps/favn_core/test/sql/` covers SQL template parsing behavior.

Use when changing manifest generation/serialization, graph planning, dependency
inference, effective execution-pool propagation, freshness keys/policies,
windows, schedules, runtime config refs, backfill range resolution, or
runner/orchestrator contract structs.

`Favn.Window.Policy` keeps schedule cadence separate from processing-window
kind and resolves explicit previous-complete or current-period anchors.
`Favn.Window.Spec.refresh_from` is not an anchor selector: under
window-success freshness it combines each exact runtime window with a calendar
refresh period through `Favn.Freshness.Key`.

`Favn.Retry.Policy` and `Favn.Retry.Backoff` own serializable node-attempt
count/timing. `Favn.Contracts.RunnerError` independently owns explicit
retryability, retry-after, and safe/unknown/cancelled outcome. Planned nodes
carry the frozen effective policy and source. `Favn.RuntimeInput.Pin`,
`Favn.RuntimeInput.Resolution`, and `Favn.Replay.InputMode` own the cross-app
pin/replay contracts without storage dependencies.

`Favn.RuntimeValue` is not a global configuration-resolution mechanism.
Integration boundaries opt in explicitly; DuckDB session-script parameters are
the first consumer. Providers return bounded errors, refs have redacted Inspect
output, and secret refs are tracked by connection redaction.

Manifest schema 7 and runner contract 7 are the only accepted versions. Static
asset and pipeline settings use `Favn.Settings`; top-level atom keys are retained
for runtime access while nested maps normalize to JSON-safe string keys.
`Favn.Run.Context`, `Favn.Run.AssetContext`, and `Favn.Run.PipelineContext`
separate asset settings, pipeline settings, submitted params, runtime config,
relation identity, and deadline data. SQL
execution payloads carry an optional typed `%Favn.SQL.Contract{}` plus typed
`%Favn.SQL.Check{}` declarations. Contract-generated checks and authored checks
share the same policy/result types and are distinguished by origin and stable
claim identity. `%Favn.SQL.ContractValidation{}` owns candidate schema
comparison and `%Favn.SQL.Contract.Diff{}` owns semantic authored-contract
comparison while fragment-composition provenance is diffed separately.
Contracts retain flattened columns, bounded `%Favn.SQL.Contract.Composition{}`
records, and typed `%Favn.SQL.Contract.Param{}` requirements. Templates reserve
`@favn_run_id` and `@favn_run_started_at` alongside the two window inputs and may
contain runtime `query()`/`target()` relation nodes,
and attempts carry bounded `%Favn.SQL.CheckResult{}` diagnostics. Older manifest
schemas and missing-graph payloads are rejected rather than upgraded.

`%Favn.SQL.SessionRequirements{version: 1}` is the manifest-safe SQL asset
physical-session contract. It stores bounded normalized string resource names
only; script locators, SQL content, resolved runtime values, and secrets remain
runtime connection config.

Runtime SQL input contracts are core-owned:
`Favn.SQLAsset.RuntimeInputs` defines the behaviour, its `Result` and `Error`
modules define typed resolver outcomes, and `Favn.RuntimeInputResolver.Ref`
defines the serializable manifest reference. `%Favn.Manifest.SQLExecution{}`
stores that reference only. Rehydration rejects malformed references and any
attempt to smuggle a resolved payload into the reference.
