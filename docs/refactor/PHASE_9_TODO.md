# Phase 9 TODO

## Status

Checklist for the next-PR-only core local developer tooling slice described in `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md`.

This list is intentionally detailed and execution-oriented. `docs/FEATURES.md` remains the high-level roadmap.

## Scope Locks

- [x] Keep this PR focused on `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, `mix favn.status`, and the minimum config/state needed to support them.
- [x] Do not pull `mix favn.install`, `mix favn.reset`, `mix favn.logs`, or build/packaging flows into this PR.
- [x] Preserve the `favn_web + favn_orchestrator + favn_runner` process boundary; do not collapse back to a same-BEAM product path.
- [x] Keep user-facing Mix tasks in `apps/favn`.
- [x] Do not introduce compile-time deps from `apps/favn` to internal runtime apps.
- [x] Keep `mix favn.dev` foreground by default; do not optimize the next PR around detached mode.

## `.favn` State And Config Foundations

- [x] Add minimal `:favn, :dev` config resolution for storage mode, SQLite path, local ports, base URLs, and local secrets overrides.
- [x] Add `.favn/runtime.json` for active stack metadata.
- [x] Add `.favn/secrets.json` for generated local service token, web session secret, and local RPC cookie.
- [x] Add `.favn/lock` lifecycle locking.
- [x] Add `.favn/logs/` log file handling.
- [x] Add `.favn/manifests/latest.json` manifest cache metadata.
- [x] Add `.favn/history/last_failure.json` failure tracking.
- [x] Record enough runtime ownership in `.favn/runtime.json` for a second terminal to run `mix favn.reload`, `mix favn.status`, and `mix favn.stop` against the foreground stack.

## Private Manifest Publish API

- [x] Add `POST /api/orchestrator/v1/manifests` so orchestrator can register a new manifest without restarting.
- [x] Protect manifest publish with the existing private service-auth boundary.
- [x] Add contract/API tests for manifest publish and activation flow.

## Local Runner/Orchestrator Control Path

- [x] Add a boundary-safe local control path for separate orchestrator and runner processes.
- [x] Keep that control path localhost-only and local-dev-scoped.
- [x] Add an orchestrator-side local runner-client implementation that can talk to a separate runner process without a compile-time runner dependency.
- [x] Allow the running foreground stack to be addressed from another terminal through project-local runtime state plus the local control path.

## `mix favn.dev`

- [x] Acquire lifecycle lock before mutating local runtime state.
- [x] Fail clearly if a partial/stale stack is already present.
- [x] Build and pin the current manifest on startup.
- [x] Start runner and verify readiness.
- [x] Start orchestrator and verify readiness.
- [x] Register the current manifest in runner.
- [x] Register and activate the current manifest in orchestrator.
- [x] Start `favn_web` as a thin built local web process and verify readiness.
- [x] Wire local orchestrator base URL and service token into `favn_web` automatically.
- [x] Default to memory storage.
- [x] Support `--sqlite` as the immediate persistent local mode.
- [x] Print URLs, pids, storage mode, and log locations on success.
- [x] Clean up newly started processes when startup fails partway through.
- [x] Keep the command attached in the foreground and stop the owned stack on interrupt or normal terminal exit.

## `mix favn.stop`

- [x] Read local runtime state from `.favn/runtime.json`.
- [x] Stop web cleanly.
- [x] Ask orchestrator to stop admitting new work and cancel local in-flight runs.
- [x] Stop runner cleanly.
- [x] Stop orchestrator cleanly.
- [x] Add timeout + forced-kill fallback behavior.
- [x] Preserve logs and SQLite data by default.
- [x] Remove only active runtime state on success.
- [x] Make the command idempotent.

## `mix favn.status`

- [x] Report whether web/orchestrator/runner are expected to be running.
- [x] Probe current pid liveness and local readiness.
- [x] Show URLs / ports where applicable.
- [x] Show storage mode.
- [x] Show current active manifest id when available.
- [x] Fall back to last cached manifest metadata when orchestrator is down.
- [x] Show recent failure/death info when a service disappeared unexpectedly.

## `mix favn.reload`

- [x] Refuse reload when the stack is not running.
- [x] Recompile the user project.
- [x] Rebuild and pin the new manifest version.
- [x] Refuse reload when in-flight runs exist.
- [x] Restart runner only for the default reload path.
- [x] Re-register the manifest in runner.
- [x] Publish/register the manifest in orchestrator through the private API.
- [x] Activate the new manifest for future runs.
- [x] Leave `favn_web` alone on default reload.
- [x] Keep orchestrator running during reload and switch it to the newly registered active manifest.
- [x] Keep browser refresh as the normal way to observe updated manifest state through `favn_web`.

## Optional Interactive Follow-Up

- [ ] Re-evaluate whether a separate interactive shell mode is needed after the normal foreground workflow lands.
- [ ] If needed later, attach only to the orchestrator/control-plane runtime.
- [ ] Add small helper functions for interactive use: `Favn.Dev.reload/0`, `Favn.Dev.status/0`, `Favn.Dev.stop/0`.

## Follow-Up Refactor / Future Features

- [ ] evaluate whether `favn` should later become a thin distribution package over authoring + local tooling ownership (`favn` + `favn_local`)
- [ ] document and enforce durable local control-plane boundaries between web, orchestrator, and runner in dev mode
- [ ] add watch mode / auto-reload
- [ ] add `doctor` / environment validation
- [ ] add `clean` / reset local state helper
- [ ] add log-tail helper commands
- [ ] add restart-single-service helper
- [ ] improve port conflict diagnostics
- [ ] define explicit `.favn/` secrets/state policy

## Testing

- [x] Add config resolution tests.
- [x] Add `.favn` state read/write tests.
- [x] Add lifecycle lock tests.
- [x] Add foreground owner-session lifecycle tests.
- [x] Add SQLite path/default tests.
- [x] Add `mix favn.dev` task behavior coverage.
- [x] Add `mix favn.stop` timeout + forced-kill coverage.
- [x] Add `mix favn.status` stale-state coverage.
- [x] Add `mix favn.reload` semantics coverage.
- [x] Add local runner/orchestrator control-path tests.
- [x] Add manifest publish API coverage.
- [x] Add one serial end-to-end local smoke path.

## Docs

- [x] Update `README.md` current-focus and doc pointers.
- [x] Update `docs/REFACTOR.md` Phase 9 wording to distinguish this next PR from later tooling/build work.
- [x] Update `docs/FEATURES.md` roadmap status for next PR versus later Phase 9 work.
- [x] Keep this plan doc up to date as implementation lands.

## Explicitly Deferred Beyond This PR

- [ ] `mix favn.install`
- [ ] `mix favn.reset`
- [ ] `mix favn.logs`
- [ ] `mix favn.build.web`
- [ ] `mix favn.build.orchestrator`
- [ ] `mix favn.build.runner`
- [ ] `mix favn.build.single`
- [ ] broader release/install/distribution polish
- [ ] durable auth/session hardening
- [ ] richer local web UX
