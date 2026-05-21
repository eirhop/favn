# favn_core

Purpose: shared compiler, manifest, planning, graph, execution policy,
freshness, window, runtime config, backfill range, and cross-runtime contract
types.

Code:
- `apps/favn_core/lib/favn/`
- `apps/favn_core/lib/favn/contracts/` owns shared runner-facing work, result,
  error, cancellation, logging, and inspection contract structs
- `apps/favn_core/lib/favn/plan/node_identity.ex` owns the planned-node identity
  seam consumed by runner work without exposing graph/index internals

Tests:
- `apps/favn_core/test/`
- App-local tests must use only `favn_core` and declared test support; authoring
  DSL parity tests live in `favn_authoring`.
- `apps/favn_core/test/sql/` covers SQL template parsing behavior.

Use when changing manifest generation/serialization, graph planning, dependency
inference, effective execution-pool propagation, freshness keys/policies,
windows, schedules, runtime config refs, backfill range resolution, or
runner/orchestrator contract structs.
