# Run UI Redesign PR Notes

## Problem

Backfills create a parent run plus child pipeline runs for each resolved data
window. The previous run UI treated those records too much like unrelated runs,
which made active backfills hard to scan and made asset/window failures hard to
diagnose.

## Architecture

- `favn_orchestrator` owns run state, persistence, read models, aggregation, and
  parent/root backfill relationships.
- `favn_view` stays thin and calls only public `FavnOrchestrator` facade
  functions for run list/detail/log data.
- `favn_view` does not call storage adapters, repos, scheduler internals, runner
  modules, or orchestrator internals directly.
- Window data is structured on asset attempts and is passed through read models;
  the UI does not parse ids, names, logs, or formatted timestamps to infer it.
- Parent backfill runs and child window runs are distinct concepts. Parent
  backfills appear as the primary `/runs` rows; child runs are shown under their
  parent or in the run detail Window runs view.
- The `/runs` overview read path is bounded and summary-oriented. It uses run
  snapshots plus the backfill-window ledger and avoids per-run event hydration;
  detail views load asset attempts and timeline data.

## UI Changes

- `/runs` is an execution-group-oriented overview with summary stats, filters,
  sorting, desktop table, mobile cards, and expandable window runs.
- `/runs/:run_id` resolves any run id to its parent/root run group and shows one
  compact run detail page.
- Run detail views include Overview, Timeline, Failures, Window runs, and Events.
- The Overview view includes an asset/window matrix and an asset attempt drawer.
- Timeline view groups rows into Running, Ran, and Queued.
- Timeline x-axis is execution wall-clock time. Data windows are shown as row
  metadata, not used as the x-axis.
- Running bars extend from `started_at` to now. Completed and failed bars use
  `started_at` to `finished_at`. Queued rows do not fabricate start times.
- Timeline supports live mode, manual zoom, fit-run overview mode, horizontal
  scroll with frozen left columns, basic mini-map, and query-backed filters.
- Storybook coverage is split by run detail view to keep the overview story fast.

## Backend And Read-Model Changes

- Added public orchestrator facade functions for execution-group summaries,
  details, asset attempts, windows, and timeline entries.
- Added orchestrator-owned aggregation for parent/root runs, child runs, asset
  attempts, windows, progress, filters, and timeline rows.
- Persisted structured window data in step event payloads so asset attempts can
  expose window metadata without UI inference.

## Testing Performed

- `apps/favn_orchestrator/test/run_read_model_test.exs` covers execution-group
  read models and window/timeline behavior.
- `apps/favn_view/test/favn_view/page_live_test.exs` covers the `/runs` overview,
  run detail views, matrix, drawer, timeline grouping, timeline modes, filters,
  and query-backed zoom state.
- Storybook was manually inspected with a realistic multi-window backfill.

## Mockup Notes

The implementation follows the attached operational Timeline mockup: dark HUD
surface, compact stats and filters, frozen asset/window/status columns, wall-clock
grid, now marker, status-colored bars, and a right-side mode rail. Sample data was
kept generic using sales/order/customer assets.

## Known Limitations

- Mini-map is basic. It shows compressed status blocks and supports click-to-jump
  in the LiveView runtime, but drag selection is deferred.
- Horizontal virtualization is basic. The chart width is bounded pragmatically;
  true horizontal virtualization is deferred.
- Vertical virtualization/pagination for very high attempt counts is deferred.
- The run overview uses a bounded recent-run scan while storage-level execution
  group pagination remains future work.
- Retry/rerun actions are not included in this PR.
- Logs/events are linked and listed, but the logs/events experience was not
  redesigned in this PR.
- Timeline viewport follow is intentionally disabled after manual scroll or zoom;
  operators must use Jump to now to re-enable live following.

## Follow-Up Ideas

- Add drag selection and viewport resizing to the mini-map.
- Add vertical virtualization or pagination for very large backfills.
- Add authorized retry/rerun actions for failed window runs and failed attempts.
- Redesign logs/events around asset attempts and window runs.
