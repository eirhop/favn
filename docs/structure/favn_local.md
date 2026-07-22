# favn_local

Purpose: customer-side artifact building and production-like local Docker
Compose lifecycle implementation behind the public `mix favn.*` tasks.

## Ownership

`favn_local` owns:

- Docker Engine and Compose prerequisite checks;
- official control-plane image resolution, digest verification, and install
  state;
- consumer-owned Compose template scaffolding and versioned Favn role labels;
- typed rendered-Compose validation plus project-scoped generated environment,
  secret, image-selection, and PostgreSQL bootstrap state;
- customer runner and aligned manifest build orchestration;
- local start, reload, status, logs, diagnostics, stop, and reset operations;
- manifest publication and activation clients;
- local run, backfill, and SQL inspection command implementation.

It does not compile, copy, or launch Favn View, Orchestrator, or PostgreSQL
storage from source. It has no runtime dependency on those applications. It
treats the installed control plane as an external OCI artifact and reaches it
through release commands and authenticated HTTP contracts.

## Primary code

- `apps/favn_local/lib/favn/dev.ex`: public implementation facade used by the
  task wrappers in `apps/favn`.
- `apps/favn_local/lib/favn/dev/install.ex`: immutable control-plane install.
- `apps/favn_local/lib/favn/dev/init/compose.ex`: non-overwriting local and
  single-host Compose starting templates.
- `apps/favn_local/lib/favn/dev/compose_deployment.ex`: typed rendered-Compose
  role and immutable-image contract.
- `apps/favn_local/lib/favn/dev/compose_project.ex`: generated interpolation,
  credential, runner-environment, and PostgreSQL bootstrap files; no YAML.
- `apps/favn_local/lib/favn/dev/compose_lifecycle.ex`: lifecycle, migration,
  deployment, reload, recovery, logs, status, and diagnostics.
- `apps/favn_local/lib/favn/dev/compose_session.ex`: authenticated local API
  session derived from the recorded runtime deployment.
- `apps/favn_local/lib/favn/dev/runner_image.ex`: runner image cache keyed by
  `runner_release_id`.
- `apps/favn_local/lib/favn/dev/build/runner.ex` and `manifest.ex`: immutable
  customer build contracts.
- `apps/favn_local/lib/favn/dev/publish.ex` and `activate.ex`: staged deployment
  operations.
- `apps/favn_local/lib/favn/dev/run.ex`, `runs.ex`, and `backfill.ex`: local
  operator workflows through the private orchestrator API.
- `apps/favn_local/lib/favn/dev/data_inspection.ex`: direct local SQL inspection
  without starting the control plane.

The small `Favn.Dev.Command` boundary runs bounded Docker and release commands.
It is not an application-process launcher.

## Tests

- Fast owning-layer tests: `apps/favn_local/test/`.
- Compose contract tests: `compose_project`, `compose_lifecycle`, install,
  runner-image, reload, and command tests in the same directory.
- Golden real-image acceptance:
  `apps/favn_local/test/acceptance/local_compose_acceptance_test.exs` and
  `local_compose_execution_acceptance_test.exs`.
- Shared canonical customer project:
  `apps/favn_local/test_support/canonical_sample_project.exs`.

The real-image cases carry only `:container` and run through
`mix test.container`; ordinary acceptance remains a separate tier.

## Invariants

- Public install resolves official GHCR releases and has no source-build or
  arbitrary-image fallback.
- Install state contains only the immutable control-plane image contract; it
  does not own Compose paths, services, credentials, ports, or resources.
- Local PostgreSQL, EPMD, and BEAM distribution ports remain private to the
  Compose network; only View and private API ports bind to loopback.
- Secrets never appear in process arguments, Compose YAML, install metadata,
  diagnostics, or persisted failure output.
- SQL-only changes do not rebuild either image. Runtime-code changes rebuild and
  replace only the customer runner after a recoverable drain boundary. The OCI
  build keeps stable dependency inputs before mutable customer inputs; it never
  copies code into a running container.
- Lifecycle commands accept only a `%Favn.Dev.ComposeDeployment{}` resolved from
  the consumer file. Stop and reset never call Compose `down`, remove orphans,
  volumes, consumer services, or `.favn/data`. If runtime state is missing after
  a partial start, project/profile/contract/role labels provide a bounded Docker
  recovery path; reset fails closed when it cannot inspect those roles.
- `runtime.json` describes only the current running Compose/deployment identity.
  `compose/selection.json` retains the last successful deployment identity so
  status, logs, diagnostics, stop, and reset keep a CLI-selected consumer file
  after the stack stops. Maintainer install metadata may identify an external
  checkout, but Favn creates no copied local source workspace, PID file, or
  host-node state.
- The production deployment command surface is `build.runner`,
  `build.manifest`, `publish`, and `activate`; local lifecycle commands are not
  production artifacts.

## Verification

From the umbrella root:

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
FAVN_CONTROL_PLANE_CANDIDATE=<loaded-candidate> mix test.container
```

Use this document when changing public local tooling, customer artifacts,
Compose lifecycle state, or the boundary between customer code and the prebuilt
control plane.
