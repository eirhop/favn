# Phase 9 TODO

## Status

Execution checklist for Phase 9 hardening, verification, and packaging honesty
work described in `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md`.

Phase 9 execution is now complete. This file is retained as a completion record
plus post-v0.5 follow-up separation.

Implemented command surface that should not be reopened as net-new feature work:

- [x] `mix favn.dev`
- [x] `mix favn.dev --sqlite`
- [x] `mix favn.stop`
- [x] `mix favn.reload`
- [x] `mix favn.status`
- [x] `mix favn.install`
- [x] `mix favn.reset`
- [x] `mix favn.logs`
- [x] `mix favn.build.web`
- [x] `mix favn.build.orchestrator`
- [x] `mix favn.build.runner`
- [x] `mix favn.build.single`
- [x] `mix favn.read_doc`

## Batch Execution Plan

Execute remaining work in bounded batches so Phase 9 can close without reopening
scope.

### Batch 1: docs/task alignment and `read_doc`

- [x] Move post-v1 niceties out of active Phase 9 execution and into roadmap sections.
- [x] Add `mix favn.read_doc ModuleName` and `mix favn.read_doc ModuleName function_name`.
- [x] Implement `read_doc` on top of `Code.fetch_docs/1` with explicit handling for:
  - `:module_not_found`
  - `:chunk_not_found`
  - unsupported or invalid docs payloads
- [x] Keep first cut limited to module docs and public function docs grouped by name.
- [x] Add/close `read_doc` tests and docs wiring.

### Batch 2: install and diagnostics hardening

- [x] Make `mix favn.dev` and `mix favn.build.*` fail clearly when install state is missing or stale.
- [x] Add stronger missing-prerequisite diagnostics for install, build, and dev flows.
- [x] Add stale-state diagnostics for local lifecycle and build flows.
- [x] Add missing Node/npm diagnostic coverage.
- [x] Add install fingerprint/stale-detection/offline-reuse coverage.

### Batch 3: lifecycle recovery hardening

- [x] Tighten lifecycle recovery when runtime state exists but all owned services are dead.
- [x] Tighten partial/dead service handling so `status` reports it clearly and `stop` stays idempotent.
- [x] Add stronger startup failure cleanup verification.
- [x] Add stale-runtime and partial/dead service recovery coverage.
- [x] Add startup-failure cleanup coverage.

### Batch 4: packaging honesty hardening

- [x] Make `mix favn.build.web` outputs operationally honest with explicit metadata-oriented semantics and operator notes.
- [x] Make `mix favn.build.orchestrator` outputs operationally honest with explicit metadata-oriented semantics and operator notes.
- [x] Verify `mix favn.build.runner` remains the reference-quality packaging target for manifest-first artifact truthfulness.
- [x] Harden `mix favn.build.single` with topology-preserving assembly-only semantics, non-operational scripts, and operator notes.
- [x] Add build metadata contract coverage for `web`, `orchestrator`, `runner`, and `single`.

### Batch 5: storage verification closeout

- [x] Add explicit SQLite verification for install, dev, reload, stop, logs, reset, and single packaging flows.
- [x] Verify explicit local Postgres configuration and startup path, not just SQLite follow-up behavior.
- [x] Add broader opt-in Postgres verification for local and orchestrator packaging paths.
- [x] Add explicit SQLite end-to-end coverage for the full Phase 9 tooling loop.
- [x] Add broader opt-in Postgres verification coverage.

Implemented Phase 9 foundation that should not be restated as open unless a real
gap remains:

- [x] Expand `.favn/` layout with `install/`, `build/`, `dist/`, manifest cache,
  and preserved failure-history directories.
- [x] Add install fingerprint metadata under `.favn/install/install.json`.
- [x] Add toolchain verification metadata under `.favn/install/toolchain.json`.
- [x] Record project-local runtime input roots for `web`, `orchestrator`, and
  `runner` in install metadata under `.favn/install/`.
- [x] Keep JS dependency state for `favn_web` under `.favn/install/`.
- [x] Add a boring reinstall path such as `mix favn.install --force`.
- [x] Record install failures under `.favn/history/`.
- [x] Define explicit Phase 9 storage modes: `memory`, `sqlite`, and `postgres`.
- [x] Keep storage selection as an orchestrator-only concern in local and
  packaged workflows.
- [x] Add public local config documentation under `config :favn, :local` for
  `storage`, `sqlite_path`, and `postgres` settings.
- [x] Preserve `mix favn.dev --sqlite` and add explicit Postgres local-dev
  invocation support.
- [x] Define runtime env contract for packaged orchestrator storage selection.
- [x] Define `mix favn.build.single --storage sqlite|postgres` semantics.
- [x] Keep web and runner deployment inputs storage-agnostic.
- [x] Require the stack to be stopped before reset deletes `.favn/`.
- [x] Auto-clear only fully stale runtime state during reset when no live owned
  services remain.
- [x] Keep the first cut of reset fully destructive with no `--keep-*` modes.
- [x] Support all-services and single-service log output, `--tail`, `--follow`,
  and historical access.
- [x] Add per-build working directories under `.favn/build/<target>/<build_id>/`.
- [x] Add final artifact directories under `.favn/dist/<target>/<build_id>/`.
- [x] Write shared `build.json` and final artifact `metadata.json` for each
  build target.
- [x] Include explicit compatibility metadata instead of relying only on
  package-version matching.
- [x] Generate and pin the current manifest during runner builds.
- [x] Package compiled user business code, pinned manifest, and plugin inventory
  into the runner artifact.
- [x] Keep one pinned manifest version per runner build.
- [x] Keep `build.web` and `build.orchestrator` free of user business code.
- [x] Keep `build.orchestrator` storage-neutral at build time.
- [x] Assemble `build.single` as separate `web`, `orchestrator`, and `runner`
  runtimes with generated assembly config and simple start/stop scripts.

## Scope Locks

- [x] Keep `apps/favn` as the public package and public `mix favn.*` entrypoint owner.
- [x] Keep `apps/favn_authoring` as authoring/manifest implementation owner.
- [x] Keep `apps/favn_local` as local lifecycle/tooling and packaging implementation owner.
- [x] Keep all Favn-managed project-local side effects under `.favn/`.
- [x] Preserve explicit `web + orchestrator + runner` runtime boundaries.
- [x] Do not require Docker by default.
- [x] Do not treat production hardening as part of this Phase 9 slice.
- [x] Do not reopen already-completed command-surface work as if it were still missing.
- [x] Do not reopen architecture with same-BEAM shortcuts.

## Remaining Hardening And Verification

- [x] Tighten lifecycle recovery when runtime state exists but all owned services are dead.
- [x] Tighten partial/dead service handling so `status` reports it clearly and `stop` can clean it up idempotently.
- [x] Add stronger startup failure cleanup verification.
- [x] Add better missing-prerequisite diagnostics for install, build, and dev flows.
- [x] Add better stale-state diagnostics for local lifecycle and build flows.
- [x] Add better port-conflict diagnostics for local startup.
- [x] Keep diagnostics targeted and boring; do not grow a broad doctor framework in this slice.

## Remaining Packaging Honesty Work

- [x] Make `mix favn.build.web` outputs operationally honest with explicit metadata-only semantics.
- [x] Make `mix favn.build.orchestrator` outputs operationally honest with explicit metadata-only semantics.
- [x] Verify `mix favn.build.runner` remains the reference-quality packaging target for manifest-first artifact truthfulness.
- [x] Harden `mix favn.build.single` with topology-preserving assembly-only semantics and explicit non-operational scripts.

## Remaining Testing

- [x] Add install fingerprint and stale-detection coverage.
- [x] Add missing Node/npm diagnostic coverage.
- [x] Add offline reuse coverage when install state is current.
- [x] Add reset refusal and success coverage.
- [x] Add logs all-services, single-service, tail, follow, and historical-access coverage.
- [x] Add build metadata contract coverage for `web`, `orchestrator`, `runner`, and `single`.
- [x] Add runner artifact coverage for user code, manifest, and plugin inclusion.
- [x] Add single artifact coverage for the three-runtime bundle layout.
- [x] Add stale-runtime recovery and partial/dead service recovery coverage.
- [x] Add startup-failure cleanup coverage.
- [x] Add explicit SQLite end-to-end coverage for the full Phase 9 tooling loop.
- [x] Add broader opt-in Postgres verification coverage.

## Docs

- [x] Keep `README.md` aligned with the implemented command surface and public local-dev flow.
- [x] Update `docs/REFACTOR.md` so Phase 9 is described as hardening, verification, and packaging honesty rather than missing commands.
- [x] Update `docs/FEATURES.md` so Phase 9 completion status and scope are explicit.
- [x] Update `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md` to reflect implemented command surface, packaging honesty, and completion status.
- [x] Keep this TODO file as a completion record plus post-v0.5 follow-up separation.

## Post-v0.5 Follow-Up

Move nice-to-have work here instead of keeping it in the active Phase 9 TODO:

- watch mode and auto-reload
- broader doctor or environment validation framework
- richer log tooling or log-query features
- restart single-service convenience flow
- convenience niceties beyond the targeted diagnostics needed for the current command surface

## Explicitly Deferred Beyond This Slice

- production hardening and safe-release auth/session work
- Docker-first local or packaging workflows
- hidden same-BEAM shortcuts for local or packaged runtimes
