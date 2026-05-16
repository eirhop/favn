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
- `apps/favn_local/lib/favn/dev/init.ex` for generated local sample files; keep
  its lakehouse sample aligned with the convention that connections are
  server/session/auth, catalogs are phases, schemas are segments/domains, and
  tables/views are assets. The bundled DuckDB smoke path attaches local `raw`
  and `mart` catalog files during connection bootstrap.
- `apps/favn_local/lib/favn/dev/env_file.ex` for local `.env` parsing/loading
  before dev/reload compile, manifest, and service launch work
- single-node bootstrap implementation under `apps/favn_local/lib/favn/dev/bootstrap/`
- `apps/favn_local/lib/favn_local.ex`
- single-node artifact integration test harness under `apps/favn_local/test_support/`
- canonical single-node acceptance sample generator under `apps/favn_local/test_support/canonical_sample_project.exs`

Tests:
- `apps/favn_local/test/`
- integration-style local tooling tests under `apps/favn_local/test/integration/`
- product-level single-node acceptance coverage under `apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs`
- single-node bootstrap tests under `apps/favn_local/test/dev_bootstrap_single_test.exs`
- orchestrator bootstrap HTTP client tests under `apps/favn_local/test/dev_orchestrator_client_test.exs`
- env-file parser/loader coverage under `apps/favn_local/test/dev_env_file_test.exs`

Test tiers:
- `:integration` means a test crosses an app, process, storage, or runtime boundary;
  it is not excluded from fast CI by itself.
- `:acceptance` means a product E2E workflow through a public user/operator path.
- `:slow` marks tests excluded from fast PR CI by default.
- `:browser` marks browser automation or browser smoke coverage excluded from fast
  PR CI by default.

Useful commands:
- Fast local-tooling slice: `MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser`
- Local acceptance suite: `MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --only acceptance`
- Full fast PR job: use the per-app commands in `.github/workflows/ci.yml`.
- Full acceptance suite: `mix test.acceptance`
- Non-acceptance slow suite: `mix test.slow`
- Test tag coverage guard: `elixir scripts/check_test_tag_tiers.exs`

Single-node artifact invariant:
- `dist_dir` is immutable after build. Runtime state must be written outside the
  artifact tree, including `runtime_home`, SQLite/DuckDB files, logs, and pid
  paths.

Use when changing `mix favn.*` local behavior, local runtime state, local HTTP
client behavior, consumer config transport, install/runtime workspaces,
single-node bootstrap, operator diagnostics, or local packaging outputs.
