# favn_local

Purpose: implementation behind local developer tooling, local stack lifecycle,
install/reset/logs/status/diagnostics/reload/run/backfill flows, local run and
SQL data inspection, PostgreSQL-backed single-node bootstrap, and packaging
commands.

Code:
- `apps/favn/lib/mix/tasks/favn.dev.ex` and `favn.reload.ex` load only the code
  paths needed for a lightweight env bootstrap before delegating configured work
  to guarded internal tasks whose `app.config` requirement evaluates consumer
  runtime configuration in a fresh Mix process
- `apps/favn_local/lib/favn/dev.ex`
- `apps/favn_local/lib/favn/dev/`
- `apps/favn_local/lib/favn/dev/local_distribution.ex` for local distributed Erlang
  loopback and EPMD preflight
- `apps/favn_local/lib/favn/dev/local_context.ex` for the shared trusted local-dev
  API context used by local CLI commands
- local orchestrator startup provisions each configured development workspace
  idempotently after the PostgreSQL backend starts and before auth/API children;
  this is local-only and never creates the database or applies migrations
- `apps/favn_local/lib/favn/dev/run.ex`, `apps/favn_local/lib/favn/dev/runs.ex`,
  and `apps/favn_local/lib/favn/dev/backfill.ex` for local operator run/backfill
  payloads and run operations, including target-aware dependency scope,
  refresh-mode validation, timeout, cancellation, successful-window rerun, and
  concurrency option forwarding
- `apps/favn_local/lib/favn/dev/init.ex` for generated local sample files; keep
  its lakehouse sample aligned with the convention that connections are
  server/session/auth, catalogs are phases, schemas are segments/domains, and
  tables/views are assets. The bundled DuckDB smoke path attaches local `raw`
  and `mart` catalog files during connection bootstrap.
- `apps/favn_local/lib/favn/dev/env_file.ex` for local `.env` parsing/loading
  and `env_bootstrap.ex` for the bounded key-only handoff into the configured
  dev/reload process before compile, manifest, and service launch work
- `apps/favn_local/lib/favn/dev/consumer_config_transport.ex` for the bounded,
  tagged local-runner handoff. It has explicit forms for
  `Favn.RuntimeConfig.Ref` and secret `Favn.RuntimeValue.Ref` values; provider
  requests remain inert bounded data and resolved credentials never enter the
  transport payload
- single-node bootstrap implementation under `apps/favn_local/lib/favn/dev/bootstrap/`
- `apps/favn_local/lib/favn_local.ex`
- single-node artifact integration test harness under `apps/favn_local/test_support/`
- canonical single-node acceptance sample generator under `apps/favn_local/test_support/canonical_sample_project.exs`
- `apps/favn_local/lib/favn/dev/build/control_plane_inputs.ex` for the selective,
  deterministic official-image input closure and
  `apps/favn_local/lib/favn/dev/build/control_plane.ex` for the repository-only,
  integrity-checked OCI context/candidate builder
- `apps/favn_local/lib/favn/dev/control_plane_image.ex` for canonical GHCR tags,
  digest references, and exact RepoDigest selection

Tests:
- `apps/favn_local/test/`
- integration-style local tooling tests under `apps/favn_local/test/integration/`
- product-level single-node acceptance coverage under `apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs`
- single-node bootstrap tests under `apps/favn_local/test/dev_bootstrap_single_test.exs`
- orchestrator bootstrap HTTP client tests under `apps/favn_local/test/dev_orchestrator_client_test.exs`
- env-file parser/loader and configured-process bootstrap coverage under
  `apps/favn_local/test/dev_env_file_test.exs` and
  `apps/favn_local/test/dev_env_bootstrap_test.exs`

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
- Full fast umbrella suite: `mix test --no-compile --timeout 1200000`.
- Full acceptance suite: `mix test.acceptance`
- Non-acceptance slow suite: `mix test.slow`
- Test tag coverage guard: `elixir scripts/check_test_tag_tiers.exs`

The root fast runner forwards ExUnit arguments to every app and reports all
failing app slices. On Unix it runs children with native `/tmp` storage so WSL
Windows-mounted temporary directories cannot change POSIX filesystem behavior.
Dependency installation, generated-consumer execution, split-root lifecycle,
and production artifact tests live only in the explicit slow or acceptance
tiers.

Single-node artifact invariant:
- `dist_dir` is immutable after build. Runtime state must be written outside the
  artifact tree, including `runtime_home`, DuckDB files, logs, and pid paths.
- The control plane is an externally managed PostgreSQL database. Bootstrap
  requires an explicit workspace and operator credentials; it never creates a
  database or silently selects a tenant.

Use when changing `mix favn.*` local behavior, local runtime state, local HTTP
client behavior, consumer config transport, install/runtime workspaces,
single-node bootstrap, operator diagnostics, local run/data inspection, local run
cancellation, local run/backfill admission options, or local packaging outputs.

Breadcrumbs:
- `Mix.Tasks.Favn.Inspect` and `Mix.Tasks.Favn.Query` own the guarded `.env`
  bootstrap for direct local SQL inspection before delegating to
  `Favn.Dev.DataInspection`; they do not start the consumer application.
- `Favn.Dev.DataInspection` owns relation parsing, connection resolution, and the
  read-only SQL guardrail used by `mix favn.inspect` and `mix favn.query`. It
  must start `:favn_sql_runtime` before opening SQL client sessions so
  `Favn.SQL.SessionPool` is supervised without requiring manual `app.start`.
