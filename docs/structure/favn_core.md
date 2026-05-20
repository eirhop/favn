# favn_core

Purpose: shared compiler, manifest, planning, graph, execution policy,
freshness, window, runtime config, backfill range, and cross-runtime contract
types.

Code:
- `apps/favn_core/lib/favn/`

Tests:
- `apps/favn_core/test/`
- App-local tests must use only `favn_core` and declared test support; authoring
  DSL parity tests live in `favn_authoring`.
- `apps/favn_core/test/sql/` covers SQL template parsing behavior.

Use when changing manifest generation/serialization, graph planning, dependency
inference, effective execution-pool propagation, freshness keys/policies,
windows, schedules, runtime config refs, backfill range resolution, or
runner/orchestrator contract structs.
