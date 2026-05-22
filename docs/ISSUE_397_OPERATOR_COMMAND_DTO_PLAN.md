# Issue 397 Operator Command DTO Plan

## Goal

Make operator run/backfill commands explicit at the orchestrator facade so
browser, API, and future CLI submit paths can pass operator intent without
reconstructing runtime execution options in `favn_view`.

## Current State

- `FavnView.AssetDetailLive` keeps browser form state, but also translates
  dependency and refresh choices into runtime submit options such as `:force`,
  `{:force_assets, refs}`, and `include_upstream: true`.
- `FavnView.AssetDetailLive` derives timeline selection ids and range requests
  before calling the orchestrator facade.
- `FavnView.PipelineDetailLive` submits pipeline backfills as plain range and
  refresh maps, with view-owned defaults.
- `FavnOrchestrator` already accepts some map-shaped submit input for manifest
  asset and pipeline commands, but the shape is still runtime-option oriented:
  `:config`, `:refresh`, `:refresh_policy`, `:dependencies`, `:window_request`,
  and `:range_request`.
- Operator auth and role checks already belong at the public orchestrator facade;
  #397 should preserve that ownership and clarify the command input immediately
  after auth succeeds.

## Design

- Add orchestrator-owned operator command DTO modules under
  `FavnOrchestrator.OperatorCommands`.
- Keep the DTOs in `favn_orchestrator`, not `favn_core`; these are control-plane
  operator command contracts, not manifest/compiler domain contracts.
- Use four small structs with moduledocs, docs, types, and `from_input/1` style
  normalization functions:
  - `AssetRunRequest`
  - `AssetBackfillRequest`
  - `PipelineRunRequest`
  - `PipelineBackfillRequest`
- Model operator intent with boring enums and maps:
  - `dependency_mode`: `:all | :none`
  - `refresh_mode`: `:auto | :missing | :force_all | :force_selected |
    :force_selected_upstream`
  - `selection`: `nil` or a struct/map containing `source`, optional stable `id`,
    and optional `kind`, `value`, `timezone`, and `run_id`
  - `range`: `kind`, `from`, `to`, and `timezone`
  - `window`: optional future pipeline window request intent for single pipeline
    runs
- Let DTO normalization accept string-keyed and atom-keyed maps because browser,
  API, and CLI callers naturally produce different input shapes.
- Return stable orchestrator-owned error atoms/tuples for invalid command input,
  such as `{:invalid_operator_dependency_mode, value}`,
  `{:invalid_operator_refresh_mode, value}`,
  `{:invalid_operator_selection_source, value}`,
  `{:invalid_operator_selection, value}`, and
  `{:invalid_operator_range, value}`.
- Translate DTOs into existing runtime submit options only inside
  `FavnOrchestrator` after manifest target resolution provides the asset ref,
  pipeline module, window policy, and selected target context.
- Keep `FavnOrchestrator.RefreshPolicy`, `RangeRequest`, and `WindowRequest` as
  runtime/control-plane internals behind the operator command boundary.
- Do not return UI labels, component state, CSS concerns, or LiveView-specific
  field names from orchestrator DTO modules.

## Public Facade Shape

- Keep the four existing operator facade functions as the public entry points:
  - `submit_operator_asset_run/4`
  - `submit_operator_asset_backfill/4`
  - `submit_operator_pipeline_run/4`
  - `submit_operator_pipeline_backfill/4`
- Change their docs and specs so the fourth argument is an explicit operator
  command input or DTO struct, not `keyword() | map()` runtime opts.
- Preserve `submit_asset_run_for_manifest/3`, `submit_pipeline_run_for_manifest/3`,
  and backfill equivalents as lower-level manifest/runtime entry points while
  moving operator-only normalization out of the LiveViews and into the operator
  facade path.
- Do not introduce backwards-compatibility aliases for old operator request shapes
  unless a concrete shipped caller needs them; this repo is private pre-v1.

## Translation Rules

- Asset run:
  - Normalize dependency and refresh modes from command input.
  - Resolve the manifest asset target before translating selected-asset refresh
    modes, because selected-asset refresh needs the canonical asset ref.
  - Translate `:force_selected_upstream` to `{:force_assets, [asset_ref],
    include_upstream: true}` only when dependency mode is `:all`; otherwise return
    the stable dependency/refresh conflict error.
  - Resolve `selection` against the asset's refresh or data-coverage timeline in
    the orchestrator, including stable id validation and window/runtime request
    translation.
- Asset backfill:
  - Normalize the operator range request into `Favn.Backfill.RangeRequest` inside
    the orchestrator.
  - Apply the same dependency and refresh intent translation as asset runs.
  - Keep backfill child refresh defaults in the orchestrator/backfill manager, not
    in the LiveView.
- Pipeline run:
  - Normalize optional window and refresh intent at the orchestrator boundary.
  - Keep pipeline module/target resolution and window-request validation inside
    the manifest-pinned facade path.
- Pipeline backfill:
  - Normalize operator range and refresh intent at the orchestrator boundary.
  - Preserve existing parent/child backfill behavior and only change the input
    contract used to reach it.

## LiveView Changes

- Keep `AssetDetailLive` responsible for browser state, selected timeline UI
  state, and local form value normalization only.
- Remove asset LiveView translation helpers that produce runtime submit options:
  `run_submit_opts/2`, `dependency_option/1`, `refresh_option/3`, and
  `backfill_refresh_option/1`.
- Stop deriving runtime force policies in the LiveView. Submit refresh mode strings
  such as `"force_selected"` and `"force_selected_upstream"` as operator intent.
- Move typed timeline-id derivation for manually entered selections from the
  LiveView into the orchestrator command translator where practical. The LiveView
  may pass backend-provided stable ids for clicked windows because those ids are
  part of the orchestrator-owned asset detail DTO.
- Keep `PipelineDetailLive` responsible for collecting `from`, `to`, `kind`,
  `timezone`, and refresh string values only.
- Keep LiveView error rendering as a mapping from stable orchestrator error atoms
  to user-facing text. Do not expose orchestrator DTO structs or runtime option
  tuples in rendered output.

## Landing Slices

1. Add orchestrator command DTO modules and unit tests for pure input
   normalization, including atom-keyed and string-keyed maps.
2. Add orchestrator facade tests that assert valid/invalid dependency modes,
   refresh modes, selected/upstream force behavior, selected timeline ids, range
   backfills, and permission failures return stable results.
3. Wire `submit_operator_asset_run/4` and `submit_operator_asset_backfill/4`
   through the new DTOs and translation functions while preserving existing
   lower-level runtime submit behavior.
4. Wire `submit_operator_pipeline_run/4` and
   `submit_operator_pipeline_backfill/4` through the new DTOs and translation
   functions.
5. Thin `AssetDetailLive` submit handlers so they pass operator command intent
   and render returned error atoms.
6. Thin `PipelineDetailLive` submit handlers the same way.
7. Update facade docs, structure docs, and feature docs to describe the operator
   command boundary.

## Tests

- DTO normalization accepts expected browser/API/CLI map shapes and rejects
  unknown dependency, refresh, selection-source, range, and window values with
  stable errors.
- Operator facade authorization remains first: unauthenticated and forbidden
  contexts fail before manifest lookup or command translation leaks target state.
- Asset run facade tests cover `:all`, `:none`, `:auto`, `:missing`,
  `:force_all`, `:force_selected`, `:force_selected_upstream`, and the
  `force_selected_upstream` plus `:none` conflict.
- Asset run facade tests cover refresh timeline and data-coverage timeline
  selection ids, including invalid ids.
- Asset and pipeline backfill facade tests cover valid ranges, invalid ranges,
  range bounds, and default refresh behavior.
- LiveView submit tests assert the view sends intent-shaped requests and renders
  returned stable errors. They should not assert runtime option tuples.

## Risks And Tradeoffs

- The operator facade currently also acts as a convenient pass-through to lower
  level manifest submit functions. Tightening the fourth argument is a breaking
  pre-v1 change, but it makes the boundary clearer and avoids duplicating refresh
  semantics in the UI.
- Some validation depends on resolved manifest targets, so DTO normalization must
  stay split between pure input parsing and context-aware translation.
- Error atoms need to be stable enough for UI/API rendering, but should remain
  domain-oriented and not mirror current component labels.
- This should not become a generic command framework. Four small DTO modules are
  enough for the current repeated contract.

## Non-Goals

- Do not change runner contracts or `Favn.Contracts.RunnerWork`.
- Do not move operator command DTOs into `favn_core`.
- Do not redesign asset or pipeline detail UI.
- Do not return UI labels, CSS classes, or component-specific state from the
  orchestrator.
- Do not change scheduler, freshness decider, or backfill manager runtime
  semantics except where required to consume the translated command options.
