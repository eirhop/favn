# Issue 182 Operational Backfill Web UI Plan

> Status: implemented for issue #182.
> Scope: add a first-class web/BFF operator surface for operational backfills using the existing private orchestrator backfill API.

## Issue Summary

Issue #182 adds the web-facing operator experience for operational backfills. The backend foundation is already implemented: parent backfill runs, child pipeline runs per resolved window, normalized backfill-window ledger rows, coverage baselines, latest asset/window state, private orchestrator HTTP endpoints, and the local `mix favn.backfill` CLI.

This issue should not redesign backfill execution. It should make the existing capability visible and operable in the SvelteKit web app through the web BFF boundary.

## Current Baseline

Implemented backend and local foundations:

- `POST /api/orchestrator/v1/backfills` submits a pipeline backfill for the active or selected manifest.
- `GET /api/orchestrator/v1/backfills/:backfill_run_id/windows` lists child-window ledger rows with bounded pagination.
- `POST /api/orchestrator/v1/backfills/:backfill_run_id/windows/rerun` reruns one failed window by `window_key`.
- `GET /api/orchestrator/v1/backfills/coverage-baselines` lists projected coverage baselines with filters and bounded pagination.
- `GET /api/orchestrator/v1/assets/window-states` lists latest projected asset/window state with filters and bounded pagination.
- `mix favn.backfill` already exercises submit, list windows, rerun failed window, coverage baseline list, and asset/window state list flows.

Existing web patterns to preserve:

- Browser components call `/api/web/v1/...` only; they never call the private orchestrator API directly.
- `src/lib/server/orchestrator.ts` owns service-token auth, actor/session forwarding, and upstream network failure handling.
- `src/lib/server/web_api.ts` owns session checks, JSON parsing, JSON error envelopes, and relay helpers.
- Existing run and asset pages use server load functions for initial page data and BFF routes for command-style interactions.
- The app shell currently treats `/runs` and `/assets` as first-class navigation roots; `/backfills` must be added deliberately.

## Product Shape

### Operator Journey

Add a new `Backfills` area for operators:

1. Open `/backfills` from the app shell.
2. Choose an active-manifest pipeline target.
3. Enter an explicit range: `from`, `to`, `kind`, and `timezone`.
4. Optionally choose a compatible coverage baseline when available.
5. Submit the backfill command and navigate to the parent run detail.
6. Inspect each window in the parent backfill ledger.
7. Rerun one failed window without resubmitting the whole backfill.
8. Inspect coverage baselines and latest asset/window state from dedicated read-model views.

The UI should present parent backfill runs as operational commands, not as ordinary ad hoc pipeline runs. It can link to the existing run inspector for run-level debugging, but the primary mental model should be range, windows, attempts, and coverage.

### Routes

Recommended page routes:

- `/backfills`: submit form, recent/known parent backfill entry points if available from run data, and links to read-model views.
- `/backfills/[run_id]`: parent backfill detail and child-window table.
- `/backfills/coverage-baselines`: coverage baseline list with filters and empty/data states.
- `/assets/window-states`: latest asset/window state list, unless integration into an asset-detail tab proves smaller and clearer.

Recommended BFF routes:

- `POST /api/web/v1/backfills`
- `GET /api/web/v1/backfills/[backfill_run_id]/windows`
- `POST /api/web/v1/backfills/[backfill_run_id]/windows/rerun`
- `GET /api/web/v1/backfills/coverage-baselines`
- `GET /api/web/v1/assets/window-states`

Keep the BFF route names parallel to the private orchestrator routes so future API-contract checks stay boring.

## Data Contracts

### Submit Backfill Request

The web submit form should produce the same request shape already used by the local CLI client:

```json
{
  "target": { "type": "pipeline", "id": "pipeline:daily_sales" },
  "manifest_selection": { "mode": "active" },
  "range": {
    "from": "2026-04-01",
    "to": "2026-04-07",
    "kind": "day",
    "timezone": "Etc/UTC"
  },
  "coverage_baseline_id": "baseline_123"
}
```

Required submit fields:

- `target.type`: must be `pipeline`.
- `target.id`: active-manifest pipeline target id.
- `manifest_selection.mode`: initially `active`.
- `range.from`: string accepted by backend range resolution for the chosen kind.
- `range.to`: string accepted by backend range resolution for the chosen kind.
- `range.kind`: `hour`, `day`, `month`, or `year`.
- `range.timezone`: IANA timezone string, defaulting to `Etc/UTC`.

Optional submit fields:

- `coverage_baseline_id`
- `max_attempts`
- `retry_backoff_ms`
- `timeout_ms`

Do not expose lookback or lookback-policy fields in this issue. The orchestrator intentionally rejects them until runtime semantics exist.

### Backfill Window Rows

Display the fields guaranteed by the current contract plus known extended fields when present:

- `backfill_run_id`
- `pipeline_module`
- `manifest_version_id`
- `window_kind`
- `window_start_at`
- `window_end_at`
- `timezone`
- `window_key`
- `status`
- `attempt_count`
- `latest_attempt_run_id`
- `last_success_run_id`
- `updated_at`

Also render optional fields when present in the current payload:

- `child_run_id`
- `coverage_baseline_id`
- `last_error`
- `started_at`
- `finished_at`
- `created_at`

Only show the rerun action for failed/error windows that have an attempt. The backend remains authoritative and may still return `409 conflict`; the UI must surface that error directly.

### Coverage Baselines

Display:

- `baseline_id`
- `pipeline_module`
- `source_key`
- `segment_key_hash`
- `window_kind`
- `timezone`
- `coverage_until`
- `created_by_run_id`
- `manifest_version_id`
- `status`
- `created_at`
- `updated_at`

The submit form should keep baseline selection optional. If no baseline exists, show an honest empty state and allow explicit range submission without a baseline.

### Asset/Window State

Display:

- `asset_ref_module`
- `asset_ref_name`
- `pipeline_module`
- `manifest_version_id`
- `window_kind`
- `window_start_at`
- `window_end_at`
- `timezone`
- `window_key`
- `status`
- `latest_run_id`
- `updated_at`

This view is read-only in issue #182.

## Boundary Design

### `web/favn_web` BFF

Add thin orchestrator client helpers in `src/lib/server/orchestrator.ts`:

- `orchestratorSubmitBackfill`
- `orchestratorListBackfillWindows`
- `orchestratorRerunBackfillWindow`
- `orchestratorListCoverageBaselines`
- `orchestratorListAssetWindowStates`

Each helper should follow the existing run/manifests helpers:

- call the private orchestrator URL from server code only
- attach service auth and actor/session headers
- preserve upstream status codes and JSON bodies where possible
- return `502 bad_gateway` for network-level upstream failures

Add BFF route handlers that:

- call `requireSession(event)`
- validate command payloads at the web boundary before forwarding
- forward read filters and pagination parameters without inventing unsupported behavior
- use `relayJson(upstream)` for normal upstream passthrough
- return the same JSON error envelope style as existing BFF routes

### `web/favn_web` Pages And Components

Add page server load functions that match existing `/runs` and `/assets` patterns:

- `/backfills/+page.server.ts` loads active manifest pipeline targets and coverage baselines.
- `/backfills/[run_id]/+page.server.ts` loads parent run detail, child windows, and active manifest summary context.
- `/backfills/coverage-baselines/+page.server.ts` loads filtered coverage baseline pages.
- `/assets/window-states/+page.server.ts` loads filtered asset/window state pages.

Recommended components:

- `BackfillsPage.svelte`: page layout, submit card, high-level operator guidance, empty states.
- `BackfillSubmitForm.svelte`: pipeline selector, range controls, timezone, optional baseline picker, submit/error/success state.
- `BackfillDetailPage.svelte`: parent summary, manifest context, window table, raw/debug payload if useful.
- `BackfillWindowsTable.svelte`: compact child-window status table and rerun action.
- `CoverageBaselinesPage.svelte`: baseline filters, table, empty/data states.
- `AssetWindowStatesPage.svelte`: asset/window filters, table, empty/data states.

Update `AppShell.svelte` so `/backfills` is a first-class nav root:

- extend the nav href type
- add a `Backfills` nav item
- update active-root detection
- update breadcrumb root detection

### Shared Web Helpers

Prefer small, parallel helpers rather than overloading existing pipeline-run submission code:

- `src/lib/backfill_submission.ts` for browser-side form normalization and submit payload construction.
- `src/lib/server/backfill_submit_payload.ts` for BFF-side validation of the accepted command shape.
- `src/lib/server/backfill_views.ts` for defensive normalization of backfill windows, coverage baselines, and asset/window states.

Reuse existing pipeline target normalization from `src/lib/pipeline_run_submission.ts` where practical, but keep backfill-specific range payloads separate from single-window run submissions.

## UX Details

### Submit Form

The form should be explicit and conservative:

- pipeline selector lists only active-manifest pipeline targets
- kind selector controls placeholder/example formatting for `from` and `to`
- timezone defaults to `Etc/UTC`
- baseline picker is optional and disabled with explanatory copy when no baselines exist
- submit button is disabled while the command is in flight
- validation errors explain which field must be fixed
- accepted submissions show the parent run id and link to `/backfills/[run_id]`

### Detail Page

The parent backfill detail should prioritize operational state:

- parent run status and manifest version
- requested range and timezone when present in run metadata
- total window count from the current page metadata if available
- status distribution from loaded rows only, labelled honestly if paginated
- child-window table with direct links to run details for `latest_attempt_run_id` and `last_success_run_id`
- one-window rerun action with confirmation copy that names the `window_key`

### Empty And Error States

Use honest states rather than hiding missing data:

- no active manifest: explain that local dev must start/register a manifest first
- active manifest has no pipelines: explain there are no pipeline targets to backfill
- no coverage baselines: explain baseline selection is optional and appears after successful coverage-producing runs
- no window rows for a parent: explain that the parent may not have planned child windows yet, or filters may exclude them
- upstream validation errors: show the orchestrator message and any field metadata

## Authorization

Match orchestrator roles:

- submit backfill: operator
- rerun failed window: operator
- list windows: viewer
- list coverage baselines: viewer
- list asset/window states: viewer

The BFF still only checks for a valid web session. The orchestrator remains authoritative for role enforcement through forwarded actor/session headers.

## Pagination And Filtering

Backend list endpoints already support `limit`, `offset`, and response pagination metadata. The web UI should expose only simple controls in the first pass:

- keep default page size aligned with backend defaults unless existing web conventions require a smaller page
- add Next/Previous controls from returned pagination metadata
- support status and pipeline filters where they are already accepted by the backend
- do not add cursor pagination, idempotency keys, advanced search, or client-only filter semantics in this issue

## Tests

### Unit Tests

Add focused Vitest coverage for:

- backfill submit payload construction
- invalid range inputs and missing pipeline target handling
- BFF submit payload validation
- normalization of window, coverage baseline, and asset/window state payloads
- pagination metadata preservation

### Storybook

Add stories for new reusable components, following existing Favn component story conventions:

- no active manifest
- no pipeline targets
- submit-ready state
- accepted submit state
- backend validation error state
- empty and populated baseline picker
- window table empty/data/failed-rerunnable states
- coverage baseline list empty/data states
- asset/window state list empty/data states

### Playwright

Extend the existing web E2E mock orchestrator and authenticated browser tests.

Minimum coverage:

- authenticated user can open `/backfills`
- unauthenticated user is redirected or receives the existing unauthorized BFF envelope
- pipeline selector renders active-manifest pipeline targets
- submitting a backfill sends the expected BFF/orchestrator payload shape
- accepted submit links to the parent backfill detail route
- detail page renders window rows
- failed-window rerun posts `window_key` to the BFF route and handles accepted response
- coverage baseline page renders empty and data states
- asset/window state page renders empty and data states

## Implementation Plan

1. Add BFF orchestrator helpers and route handlers for the five backfill endpoints, with focused route tests or E2E API assertions where existing patterns support them.
2. Add backfill payload/view normalization helpers and unit tests before building Svelte components.
3. Extend the app shell navigation and breadcrumb root handling for `/backfills`.
4. Build `/backfills` with active-manifest pipeline target loading, optional baseline loading, and the submit form.
5. Build `/backfills/[run_id]` with parent run context, window list loading, and one-window rerun action.
6. Build `/backfills/coverage-baselines` with filter, pagination, empty, and data states.
7. Build `/assets/window-states` or the smaller asset-detail integration if that proves more consistent after implementation starts.
8. Add Storybook stories for every new reusable component and run focused story checks.
9. Extend Playwright mock orchestrator routes and add the minimum issue #182 E2E coverage.
10. Update `README.md`, `docs/FEATURES.md`, and `docs/ROADMAP.md` when the UI lands; until then, keep this plan linked from the roadmap only.

## Out Of Scope

- Backend persistence or projection model changes.
- CLI changes to `mix favn.backfill`.
- Lookback policy input.
- Idempotency keys.
- Advanced filtering not already supported by the private orchestrator API.
- Production auth/session persistence hardening.
- General SQL/data inspection beyond already implemented safe relation previews.

## Open Questions

- Should `/assets/window-states` be a standalone route for operators, or should it be folded into asset detail once the first implementation shows the smallest clear UI?
- Should the first `/backfills` page include recent parent backfill runs if run metadata can identify them cheaply, or should parent discovery wait until a dedicated run filter exists?
- Should baseline picker filtering be limited to the selected pipeline and range kind in the first pass, or should it show all baselines with enough metadata for operators to choose manually?
