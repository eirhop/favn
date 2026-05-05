# favn_local

Purpose: implementation behind local developer tooling, local stack lifecycle,
install/reset/logs/status/diagnostics/reload/run/backfill flows, single-node bootstrap, and
packaging commands, including the project-local backend-only SQLite
`build.single` launcher.

Code:
- `apps/favn_local/lib/favn/dev.ex`
- `apps/favn_local/lib/favn/dev/`
- `apps/favn_local/lib/favn/dev/local_distribution.ex` for local distributed Erlang
  loopback and EPMD preflight
- `apps/favn_local/lib/favn/dev/local_context.ex` for the shared trusted local-dev
  API context used by local CLI commands
- `apps/favn_local/lib/favn/dev/env_file.ex` for local `.env` parsing/loading
  before dev/reload compile, manifest, and service launch work
- single-node bootstrap implementation under `apps/favn_local/lib/favn/dev/bootstrap/`
- `apps/favn_local/lib/favn_local.ex`
- single-node artifact integration test harness under `apps/favn_local/test_support/`

Tests:
- `apps/favn_local/test/`
- integration-style local tooling tests under `apps/favn_local/test/integration/`
- single-node artifact runtime smoke coverage under `apps/favn_local/test/integration/single_node_artifact_runtime_test.exs`
- single-node first-run bootstrap E2E coverage under `apps/favn_local/test/integration/single_node_bootstrap_e2e_test.exs`
- single-node bootstrap tests under `apps/favn_local/test/dev_bootstrap_single_test.exs`
- orchestrator bootstrap HTTP client tests under `apps/favn_local/test/dev_orchestrator_client_test.exs`
- env-file parser/loader coverage under `apps/favn_local/test/dev_env_file_test.exs`

Use when changing `mix favn.*` local behavior, local runtime state, local HTTP
client behavior, consumer config transport, install/runtime workspaces,
single-node bootstrap, operator diagnostics, or local packaging outputs.
