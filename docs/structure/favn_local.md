# favn_local

Purpose: implementation behind local developer tooling, local stack lifecycle,
install/reset/logs/status/reload/run/backfill flows, and packaging commands.

Code:
- `apps/favn_local/lib/favn/dev.ex`
- `apps/favn_local/lib/favn/dev/`
- `apps/favn_local/lib/favn_local.ex`

Tests:
- `apps/favn_local/test/`
- integration-style local tooling tests under `apps/favn_local/test/integration/`

Use when changing `mix favn.*` local behavior, local runtime state, local HTTP
client behavior, consumer config transport, install/runtime workspaces, or local
packaging outputs.
