# Phase 9 TODO

## Status

Checklist for the remaining developer-tooling and packaging slice described in
`docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md`.

Completed baseline work that should not be reopened here:

- [x] `mix favn.dev`
- [x] `mix favn.dev --sqlite`
- [x] `mix favn.stop`
- [x] `mix favn.reload`
- [x] `mix favn.status`

## Scope Locks

- [x] Keep `apps/favn` as the public package and public `mix favn.*` entrypoint owner.
- [x] Keep `apps/favn_authoring` as authoring/manifest implementation owner.
- [x] Keep `apps/favn_local` as local lifecycle/tooling and packaging implementation owner.
- [x] Keep all Favn-managed project-local side effects under `.favn/`.
- [x] Preserve explicit `web + orchestrator + runner` runtime boundaries.
- [x] Do not require Docker by default.
- [x] Do not treat production hardening as part of this Phase 9 slice.
- [x] Do not reopen already-completed `dev`, `stop`, `reload`, and `status` work.

## Install Foundation

- [ ] Expand `.favn/` layout with `install/`, `build/`, `dist/`, manifest cache, and preserved failure-history directories.
- [ ] Add install fingerprint metadata under `.favn/install/install.json`.
- [ ] Add toolchain verification metadata under `.favn/install/toolchain.json`.
- [ ] Materialize project-local runtime inputs for `web`, `orchestrator`, and `runner` under `.favn/install/runtimes/`.
- [ ] Keep JS dependency state for `favn_web` under `.favn/install/`.
- [ ] Implement `mix favn.install` as an explicit setup command.
- [ ] Make `mix favn.dev` and `mix favn.build.*` fail clearly when install state is missing or stale.
- [ ] Add a boring reinstall path such as `mix favn.install --force`.
- [ ] Record install failures under `.favn/history/`.

## Reset And Logs

- [ ] Implement `mix favn.reset`.
- [ ] Require the stack to be stopped before reset deletes `.favn/`.
- [ ] Auto-clear only fully stale runtime state during reset when no live owned services remain.
- [ ] Keep the first cut of reset fully destructive with no `--keep-*` modes.
- [ ] Implement `mix favn.logs` as a thin file-tail helper.
- [ ] Support all-services log output.
- [ ] Support single-service selection for `web`, `orchestrator`, and `runner`.
- [ ] Support `--tail`.
- [ ] Support `--follow`.
- [ ] Preserve historical log access when the stack is down.

## Build Metadata Foundations

- [ ] Add per-build working directories under `.favn/build/<target>/<build_id>/`.
- [ ] Add final artifact directories under `.favn/dist/<target>/<build_id>/`.
- [ ] Write shared `build.json` metadata for each build target.
- [ ] Write final artifact `metadata.json` for each build target.
- [ ] Include explicit compatibility metadata instead of relying only on package-version matching.

## `mix favn.build.runner`

- [ ] Build a runner artifact from the current user project plus installed internal runner runtime inputs.
- [ ] Generate and pin the current manifest through `favn_authoring` during the build.
- [ ] Package compiled user business code into the runner artifact.
- [ ] Package the pinned serialized manifest into the runner artifact.
- [ ] Package selected plugins into the runner artifact as appropriate.
- [ ] Write runner metadata including manifest schema version, manifest version id, manifest hash, runner contract version, and plugin inventory.
- [ ] Keep the first cut to one pinned manifest version per runner build.

## `mix favn.build.web`

- [ ] Build a deployable web artifact from installed `favn_web` runtime inputs.
- [ ] Keep the web artifact free of user business code.
- [ ] Write web metadata including supported orchestrator API compatibility and required environment contract.

## `mix favn.build.orchestrator`

- [ ] Build a deployable orchestrator artifact from installed internal runtime inputs.
- [ ] Keep the orchestrator artifact free of user business code.
- [ ] Keep the build artifact storage-neutral at build time.
- [ ] Write orchestrator metadata including API compatibility, runner compatibility, and storage expectations.

## `mix favn.build.single`

- [ ] Assemble one single-node bundle containing separate `web`, `orchestrator`, and `runner` runtimes.
- [ ] Generate one assembly manifest for config wiring across the three runtimes.
- [ ] Generate per-service env/config files in the single bundle.
- [ ] Generate simple start/stop scripts for the single bundle.
- [ ] Default single-node storage to SQLite.
- [ ] Keep explicit runtime boundaries visible in the output layout and metadata.

## Validation And Polish

- [ ] Tighten lifecycle recovery when runtime state exists but all owned services are dead.
- [ ] Tighten partial/dead service handling so `status` reports it clearly and `stop` can clean it up idempotently.
- [ ] Add stronger startup failure cleanup verification.
- [ ] Add explicit SQLite follow-up verification for install, dev, logs, reset, and single packaging flows.
- [ ] Add broader opt-in Postgres verification for local and orchestrator packaging paths.
- [ ] Add better missing-prerequisite diagnostics for install/build/dev flows.
- [ ] Add better port-conflict diagnostics for local startup.
- [ ] Keep diagnostics targeted and boring; do not grow a broad doctor framework in this slice.

## Testing

- [ ] Add install fingerprint and stale-detection coverage.
- [ ] Add missing Node/npm diagnostic coverage.
- [ ] Add offline reuse coverage when install state is current.
- [ ] Add reset refusal and success coverage.
- [ ] Add logs all-services, single-service, tail, follow, and historical-access coverage.
- [ ] Add build metadata contract coverage for `web`, `orchestrator`, `runner`, and `single`.
- [ ] Add runner artifact coverage for user code, manifest, and plugin inclusion.
- [ ] Add single artifact coverage for the three-runtime bundle layout.
- [ ] Add stale-runtime recovery and partial/dead service recovery coverage.
- [ ] Add explicit SQLite end-to-end coverage for the full Phase 9 tooling loop.
- [ ] Add broader opt-in Postgres verification coverage.

## Docs

- [ ] Update `README.md` first-time setup docs when `mix favn.install` lands.
- [ ] Update `README.md` packaging docs for split vs single deployment.
- [ ] Keep `docs/REFACTOR.md` Phase 9 wording aligned with the remaining tooling/packageability scope.
- [ ] Keep `docs/FEATURES.md` roadmap wording aligned with the remaining Phase 9 batch.
- [ ] Keep `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md` current as implementation lands.
- [ ] Use this TODO file as the execution checklist for the remaining Phase 9 slice.

## Explicitly Deferred Beyond This Slice

- [ ] production hardening and safe-release auth/session work
- [ ] Docker-first local or packaging workflows
- [ ] broad environment doctor framework
- [ ] richer observability or structured log-query tooling
- [ ] hidden same-BEAM shortcuts for local or packaged runtimes
