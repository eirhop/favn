# favn_core

Purpose: shared compiler, manifest, planning, graph, execution policy,
freshness, window, runtime config refs/bundles/field-level merge contracts, backfill range, and cross-runtime contract
types.

Code:
- `apps/favn_core/lib/favn/`
- `apps/favn_core/lib/favn/contracts/` owns shared runner-facing work, result,
  error, cancellation, logging, and inspection contract structs
- `apps/favn_core/lib/favn/plan/node_identity.ex` owns the planned-node identity
  seam consumed by runner work without exposing graph/index internals
- `apps/favn_core/lib/favn/manifest/labels.ex` owns tag/category label
  normalization so selector-facing metadata persists and matches as strings

Tests:
- `apps/favn_core/test/`
- App-local tests must use only `favn_core` and declared test support; authoring
  DSL parity tests live in `favn_authoring`.
- `apps/favn_core/test/sql/` covers SQL template parsing behavior.

Use when changing manifest generation/serialization, graph planning, dependency
inference, effective execution-pool propagation, freshness keys/policies,
windows, schedules, runtime config refs, backfill range resolution, or
runner/orchestrator contract structs.

Manifest schema 4 and runner contract 4 are the only accepted versions. SQL
execution payloads carry typed `%Favn.SQL.Check{}` declarations, templates may
contain runtime `query()`/`target()` relation nodes, and attempts carry bounded
`%Favn.SQL.CheckResult{}` diagnostics. Older manifest schemas and missing-graph
payloads are rejected rather than upgraded.

Runtime SQL input contracts are core-owned:
`Favn.SQLAsset.RuntimeInputs` defines the behaviour, its `Result` and `Error`
modules define typed resolver outcomes, and `Favn.RuntimeInputResolver.Ref`
defines the serializable manifest reference. `%Favn.Manifest.SQLExecution{}`
stores that reference only. Rehydration rejects malformed references and any
attempt to smuggle a resolved payload into the reference.
