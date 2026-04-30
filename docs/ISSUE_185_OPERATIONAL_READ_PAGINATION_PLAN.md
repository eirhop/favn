# Issue 185 Operational Read Pagination Plan

> Status: implemented for issue #185.
> Scope: add bounded offset pagination to operational backfill read endpoints, storage adapters, local CLI helpers, and HTTP contracts.

Issue: <https://github.com/eirhop/favn/issues/185>

## Goal

Make operational backfill reads safe as read-model counts grow. The affected reads are:

- backfill windows: `GET /api/orchestrator/v1/backfills/:backfill_run_id/windows`
- coverage baselines: `GET /api/orchestrator/v1/backfills/coverage-baselines`
- asset/window states: `GET /api/orchestrator/v1/assets/window-states`
- local helpers and CLI commands that call those endpoints

Every external read path should enforce an explicit default limit and maximum limit, return pagination metadata, and keep local CLI output bounded.

## Current Baseline

Implemented foundations already present:

- `FavnOrchestrator.API.Router` parses read filters and returns `%{items: [...]}` for all three endpoints.
- `FavnOrchestrator.list_backfill_windows/1`, `list_coverage_baselines/1`, and `list_asset_window_states/1` delegate directly to `FavnOrchestrator.Storage`.
- `Favn.Storage.Adapter` defines list callbacks for coverage baselines, backfill windows, and asset/window states.
- Memory, SQLite, and Postgres adapters already support filter predicates and deterministic SQL ordering for most reads.
- `Favn.Dev.OrchestratorClient` has shared request helpers for item-list responses.
- `mix favn.backfill windows`, `coverage-baselines`, and `asset-window-states` expose the local operator read surface.
- HTTP contract tests already lock individual item schemas for the three read models.

Important gaps to close:

- The three HTTP endpoints return unpaginated collections.
- Storage list callbacks return raw lists, so callers cannot know whether more rows exist.
- Pagination controls are not validated at the API edge or local CLI edge.
- Memory adapter list order depends on map enumeration unless explicitly sorted.
- SQLite and Postgres asset/window-state ordering differs today, which is unsafe for consistent pagination.
- `find_backfill_window/2` uses the list path for a single-row lookup and should move to the existing `get_backfill_window/3` storage function before list semantics change.
- Local CLI output has no bounded default or follow-up instruction for the next page.

## Architectural Decision

Use offset pagination for V1.

Offset pagination is the smallest correct design for these private operational reads. It keeps the HTTP contract obvious, maps directly to SQLite/Postgres, is easy to implement in the memory adapter, and is sufficient for local operator tooling. Cursor pagination can be added later if deep-page performance or mutation drift becomes a real problem.

Do not expose total counts in V1. Counting would add database work that is not needed to bound responses. Instead, each adapter should fetch `limit + 1` rows, return only `limit` items, and compute `has_more` plus `next_offset` from the extra row.

## Pagination Contract V1

External query parameters:

```text
limit=100
offset=0
```

Rules:

- Default `limit` to `100`.
- Cap `limit` at `500`.
- Require `limit` to be an integer from `1` through `500`.
- Default `offset` to `0`.
- Require `offset` to be a non-negative integer.
- Return `422 validation_failed` for invalid pagination parameters.

HTTP response shape:

```json
{
  "data": {
    "items": [],
    "pagination": {
      "limit": 100,
      "offset": 0,
      "has_more": false,
      "next_offset": null
    }
  }
}
```

`next_offset` should be `offset + item_count` when `has_more` is true, otherwise `null`.

## Storage Contract V1

Add a small shared page shape under `favn_orchestrator`, for example:

```elixir
%FavnOrchestrator.Page{
  items: [...],
  limit: 100,
  offset: 0,
  has_more?: false,
  next_offset: nil
}
```

Change the three read-model list callbacks to return a page, not a raw list:

```elixir
{:ok, %FavnOrchestrator.Page{items: items}}
```

The list query keyword should carry both filters and pagination controls. Adapters must treat `:limit` and `:offset` as query controls, not filter columns.

Storage facade responsibilities:

- Normalize and validate `:limit` and `:offset` once before adapter calls.
- Apply the same default and maximum for all adapters.
- Keep unsupported filter errors intact.
- Update specs and docs for the changed return shape.

Adapter responsibilities:

- Apply stable ordering before pagination.
- Fetch `limit + 1` rows and trim to `limit`.
- Return the shared page shape.
- Keep filter semantics identical across memory, SQLite, and Postgres.

Canonical ordering:

- coverage baselines: `updated_at DESC, baseline_id ASC`
- backfill windows: `window_start_at ASC, backfill_run_id ASC, pipeline_module ASC, window_key ASC`
- asset/window states: `updated_at DESC, asset_ref_module ASC, asset_ref_name ASC, window_key ASC`

## Boundary Plan

### `favn_orchestrator`

- Add shared pagination/page helpers.
- Update `Favn.Storage.Adapter` callback specs for paginated read-model lists.
- Update `FavnOrchestrator.Storage` and `FavnOrchestrator` specs/docs to return page structs.
- Update memory adapter filtering to sort before applying offset/limit.
- Update SQLite and Postgres query builders to append `LIMIT` and `OFFSET` after canonical ordering.
- Move backfill-window rerun lookup to `Storage.get_backfill_window/3` instead of list filtering.
- Update router pagination parsing and response DTOs.
- Add HTTP contract schema coverage for the pagination metadata and the paginated response envelopes.

### `favn_storage_sqlite`

- Apply the shared pagination query controls in all three read-model list queries.
- Align asset/window-state ordering with the canonical order.
- Add adapter tests for default limit, explicit limit/offset, max enforcement through facade, and `has_more` metadata.

### `favn_storage_postgres`

- Apply the shared pagination query controls in all three read-model list queries.
- Align backfill-window ordering with the canonical order if needed.
- Add live adapter coverage for explicit limit/offset and metadata when a Postgres test database is available.

### `favn_local` And `favn`

- Change local orchestrator client list helpers to return paginated responses, not bare item lists.
- Add `--limit` and `--offset` to `mix favn.backfill windows`, `coverage-baselines`, and `asset-window-states`.
- Keep CLI defaults bounded through the HTTP default.
- Print a short follow-up hint when `has_more` is true, including the next `--offset` value.

### Documentation

- Update `docs/FEATURES.md` after implementation because bounded operational reads are user-visible.
- Update `README.md` for the local `mix favn.backfill` read commands and pagination flags.
- Keep this plan as the implementation checklist until issue #185 is complete.

## Implementation Tasks

- [x] Add shared page/pagination normalization in `favn_orchestrator`.
- [x] Update storage behaviour, facade, and public orchestrator specs.
- [x] Implement paginated memory adapter reads with canonical ordering.
- [x] Implement paginated SQLite adapter reads with canonical ordering.
- [x] Implement paginated Postgres adapter reads with canonical ordering.
- [x] Update router parsing and DTO response envelopes.
- [x] Add or update HTTP contract schemas for pagination metadata.
- [x] Update `favn_local` orchestrator client and backfill helpers.
- [x] Update `mix favn.backfill` parsing, output, and tests.
- [x] Update `README.md` and `docs/FEATURES.md` once behavior is implemented.
- [x] Run focused tests first, then the full Elixir gate before finishing implementation.

## Verification Plan

Focused checks:

- `mix test apps/favn_orchestrator/test/api/router_test.exs`
- `mix test apps/favn_orchestrator/test/http_contract/schema_test.exs`
- `mix test apps/favn_orchestrator/test/storage_facade_test.exs`
- `mix test apps/favn_storage_sqlite/test/sqlite_storage_test.exs`
- `mix test apps/favn_local/test/dev_orchestrator_client_test.exs`
- `mix test apps/favn/test/mix_tasks/public_tasks_test.exs`

Full gate before finishing Elixir changes:

```bash
mix format
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix xref graph --format stats --label compile-connected
```
