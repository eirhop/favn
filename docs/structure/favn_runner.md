# favn_runner

Purpose: execution runtime for pinned manifest work, runner server, worker
execution, plugin loading, runtime context construction, and safe relation
inspection.

Code:
- `apps/favn_runner/lib/favn_runner.ex`
- `apps/favn_runner/lib/favn_runner/`
- runner-owned execution modules under `apps/favn_runner/lib/favn/`

Tests:
- `apps/favn_runner/test/`
- App-local tests use manifest-shaped fixtures and fake SQL adapters instead of
  authoring DSL fixtures or concrete runner plugins from sibling apps.
- Connection loader tests declare the authoring app as a test-only dependency so
  local connection fixtures can use the public `Favn.Connection` behaviour.

Use when changing asset execution, runner protocol behavior, cancellation,
timeouts, manifest registration/resolution, plugin config, SQL asset execution,
or runner-owned inspection.
