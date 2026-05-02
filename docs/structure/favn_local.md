# favn_local

Purpose: implementation behind local developer tooling, local stack lifecycle,
install/reset/logs/status/diagnostics/reload/run/backfill flows, single-node bootstrap, and
packaging commands, including the project-local backend-only SQLite
`build.single` launcher.

Code:
- `apps/favn_local/lib/favn/dev.ex`
- `apps/favn_local/lib/favn/dev/`
- single-node bootstrap implementation under `apps/favn_local/lib/favn/dev/bootstrap/`
- `apps/favn_local/lib/favn_local.ex`

Tests:
- `apps/favn_local/test/`
- integration-style local tooling tests under `apps/favn_local/test/integration/`
- single-node bootstrap tests under `apps/favn_local/test/dev_bootstrap_single_test.exs`
- orchestrator bootstrap HTTP client tests under `apps/favn_local/test/dev_orchestrator_client_test.exs`

Use when changing `mix favn.*` local behavior, local runtime state, local HTTP
client behavior, consumer config transport, install/runtime workspaces,
single-node bootstrap, operator diagnostics, or local packaging outputs.
