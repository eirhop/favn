# Issue 398 Runtime Dependency Contract Plan

## Goal

Make orchestrator runtime dependencies explicit at the application boundary so
runner, storage, and log-redaction behavior does not change implicitly after
startup when application env mutates.

## Current Landing Slice

- Add `FavnOrchestrator.RuntimeConfig` as an internal normalized struct and
  supervised process.
- Keep application env as the boot-time input for production config and local-dev
  launch ergonomics.
- Start `RuntimeConfig` before storage, run recovery, run manager, scheduler, and
  API children.
- Route runner client/options, storage adapter/options, and log-redaction policy
  reads through `RuntimeConfig.current/1`.
- Preserve public facade function signatures and adapter callback shapes.

## Design

- `FavnOrchestrator.Application` applies production env config, normalizes one
  runtime config struct, uses it to build storage child specs, then supervises the
  same struct for runtime lookups.
- `FavnOrchestrator.Storage` remains the storage boundary. Its public helper
  functions now read adapter dependencies from the explicit runtime config.
- `FavnOrchestrator`, `RunManager`, `RunServer.Execution`, readiness, and
  diagnostics resolve runner dependencies from the runtime config instead of
  directly reading application env.
- The compatibility fallback in `RuntimeConfig.current/1` normalizes from app env
  when the orchestrator supervision tree is not running, preserving isolated unit
  test and helper behavior.
- `config/test.exs` keeps the default runtime config name in dynamic-env mode so
  existing app-env based tests remain small. Tests that need startup-freeze
  semantics can start a named `RuntimeConfig` process directly.

## Follow-Up Scope

- Move scheduler and API server options into the same contract if they become hot
  runtime dependencies or need post-start mutation protection.
- Convert `FavnOrchestrator.ProductionRuntimeConfig` and `Favn.Dev.RuntimeLaunch`
  from application env writes to direct runtime-config assembly once launch flows
  can pass a single boot contract without broad churn.
- Add a runtime launch acceptance test that starts the local orchestrator and
  verifies post-start env mutation does not affect active storage/runner calls.

## Tests

- Unit coverage for runtime config normalization.
- Regression coverage that a supervised runtime config remains stable after app
  env mutation.
- Existing readiness, diagnostics, run manager, run server, and storage tests
  should continue to exercise the same public behavior through the new contract.
