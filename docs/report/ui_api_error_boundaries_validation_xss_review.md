# UI/API Error Boundaries, Validation, Sanitization, and XSS Review

Date: 2026-05-24

Scope reviewed:

- `docs/structure/README.md`
- `docs/structure/favn_view.md`
- `docs/structure/favn_orchestrator.md`
- `docs/structure/favn_core.md`
- `docs/structure/favn.md`
- `docs/structure/favn_authoring.md`
- `apps/favn_view`
- `apps/favn_orchestrator`
- `apps/favn_core`
- `apps/favn`
- `apps/favn_authoring`

Review method: static source inspection after rebasing the review worktree on `origin/main`. Tidewave runtimes were not confirmed running, so runtime behavior was not inspected.

## Blocking Security Issues

### SSE event field injection is possible from string event types

File: `apps/favn_orchestrator/lib/favn_orchestrator/storage/run_event_codec.ex`

Function: `FavnOrchestrator.Storage.RunEventCodec.validate_event_type/1`

Lines: 83-85

File: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`

Function: `FavnOrchestrator.API.Router.sse_run_event_body/3`

Lines: 2640-2644

Why risky: run event normalization accepts any non-empty binary as `event_type`. The SSE response then interpolates that value directly into the line-oriented `event:` field. A persisted or injected event type containing CR/LF can forge additional SSE fields or events in downstream clients.

Concrete fix: restrict event types to known atoms or a strict line-safe regex at normalization/decode time, for example `~r/\A[a-zA-Z0-9_.:-]{1,128}\z/`. Add a narrow SSE field encoder/rejector for `event` and `id` values before chunking, and add regression tests for CR, LF, and multi-line injection attempts.

### Internal error reasons are exposed in browser/API responses

File: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`

Function: `FavnOrchestrator.API.Router` run submit fallback

Lines: 693-695

File: `apps/favn_view/lib/favn_view/runs_list_live.ex`

Functions: `load_groups/1`, `ensure_group_detail/2`

Lines: 167-171, 288-294

File: `apps/favn_view/lib/favn_view/pipeline_detail_live.ex`

Function: `submit_error_label/1`

Lines: 403-405

File: `apps/favn_view/lib/favn_view/logs_live_support.ex`

Function: `error_label/1`

Lines: 276-277

Why risky: `inspect(reason)` is returned or rendered to users. Today many reasons are atoms/tuples, but storage, adapter, SQL, exception, or manifest errors can include paths, SQL, connection details, internal module names, IDs, or future secrets. This also makes user-facing error contracts unstable.

Concrete fix: map known errors to stable atoms/messages at the boundary that owns the operation. Log detailed reasons server-side with request/run IDs. Browser/API responses should use stable messages such as `Request failed`, `Run could not be loaded`, or a known validation label, without embedding `inspect(reason)`.

## User-Facing Crash/Error-Boundary Issues

### JSON API body shape is not validated before map access

File: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`

Functions: `fetch_json_body/1`, `fetch_required_string/2`

Lines: 1528-1536

Example route: `POST /api/orchestrator/v1/auth/password/sessions`

Lines: 101-106

Why risky: `fetch_json_body/1` returns `conn.body_params` without checking it is a map. Routes then pass that value to helpers that call `Map.get/3`. A syntactically valid JSON array, string, number, or boolean body can crash the request instead of returning a stable validation response.

Concrete fix: make `fetch_json_body/1` return `{:ok, params}` only for maps and `{:error, :invalid_json_body}` otherwise. Handle `:invalid_json_body` as a `422 validation_failed` response in all JSON body routes.

### Idempotency completion failure can crash after command execution

File: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`

Function: `FavnOrchestrator.API.Router.execute_idempotent_command/4`

Lines: 1606-1635

Why risky: `:ok = Idempotency.complete(...)` pattern matches both success and failure responses. If the command executes but idempotency persistence fails, the request crashes after the mutation, creating an ambiguous user-facing failure and weakening replay semantics.

Concrete fix: handle `{:error, reason}` explicitly. Log the idempotency write failure with operation and resource IDs. Return a stable `500 internal_error` response that states the command outcome is unknown, or persist an explicit compensating failure state if the storage contract supports it. Add a regression test with an adapter that fails completion after reservation.

### Run event query params silently ignore invalid values and allow unbounded reads

File: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`

Function: `FavnOrchestrator.API.Router.run_event_opts/1`

Lines: 2099-2128

Why risky: malformed `after_sequence` and `limit` strings are silently ignored, so invalid user input can change behavior without a validation error. Missing `limit` passes no bound to the facade; `FavnOrchestrator.validate_run_event_opts/1` only rejects invalid present limits and does not enforce a default maximum.

Concrete fix: parse `after_sequence` and `limit` with strict helpers that return `{:error, :invalid_opts}` on malformed values. Apply a bounded default and max limit at the API boundary, for example default `100`, max `500`, and return `422` for out-of-range values.

### Detail pages collapse backend failures into not-found states

File: `apps/favn_view/lib/favn_view/asset_detail_live.ex`

Function: `load_asset/1`

Lines: 286-292

File: `apps/favn_view/lib/favn_view/pipeline_detail_live.ex`

Function: `load_pipeline/1`

Lines: 151-157

Why risky: every orchestrator error becomes `nil`, so storage outages, invalid active manifests, and backend failures render as `Asset not found` or `Pipeline not found`. Operators get misleading remediation and no clear error boundary.

Concrete fix: return a view model such as `{:ok, detail}`, `{:not_found, id}`, or `{:error, stable_reason}`. Render separate not-found and backend-unavailable panels, and keep detailed reasons out of the UI.

## Boundary Violations

No direct `favn_view` calls to storage adapters, repos, runner modules, scheduler internals, persistence modules, compiler internals, or plugin internals were found in the inspected source. Calls are through `FavnOrchestrator` facade functions.

Potential boundary cleanup:

File: `apps/favn_view/lib/favn_view/logs_live_support.ex`

Module use: `Favn.Log.Filter`

Line: 7

Why it matters: this appears to be a shared log filter contract rather than backend behavior, so it is not a direct internal coupling. If the intended UI boundary is stricter, expose a log filter DTO constructor or public filter shape through `FavnOrchestrator` and keep `favn_view` on facade-owned shapes only.

## Worthwhile Cleanup

### UI form params are copied before narrow local validation

File: `apps/favn_view/lib/favn_view/asset_detail_live.ex`

Function: `run_config_from_params/2`

Lines: 533-542

File: `apps/favn_view/lib/favn_view/pipeline_detail_live.ex`

Function: `backfill_config/1`

Lines: 202-209

Why risky: orchestrator DTOs catch many invalid values, but the UI boundary should still validate form shape and allowed choices before command submission. Without that, users get delayed generic failures and the LiveView carries invalid form state as if it were accepted input.

Concrete fix: add narrow validation in each LiveView for allowed `dependencies`, `refresh`, `kind`, non-empty range fields, and basic timezone shape. Keep orchestration semantics in `FavnOrchestrator`; the UI should only validate interaction shape and render stable local errors.

### Pipeline backfill DTO does not validate numeric option fields

File: `apps/favn_orchestrator/lib/favn_orchestrator/operator_commands/pipeline_backfill_request.ex`

Function: `from_input/1`

Lines: 39-54

Why risky: `max_attempts`, `retry_backoff_ms`, `timeout_ms`, and `coverage_baseline_id` are copied directly into the DTO. Later runtime layers validate some values, but the operator command DTO should own required and allowed command input shape.

Concrete fix: add narrow DTO validation for positive integer `max_attempts`, non-negative integer `retry_backoff_ms`, positive integer `timeout_ms`, and non-empty binary `coverage_baseline_id`. Return explicit `{:invalid_operator_*}` errors and map them to `422` in API/UI callers.

## XSS/Sanitization Notes

No direct HEEx/JavaScript XSS sink was found in the inspected app code:

- no `Phoenix.HTML.raw/1`
- no `raw(...)`
- no `{:safe, ...}`
- no `innerHTML`
- no `insertAdjacentHTML`
- no React-style `dangerouslySetInnerHTML`

Rendered manifest labels, log messages, output metadata, run errors, and event summaries use HEEx interpolation or HTML/data attributes, which Phoenix escapes. The remaining concrete sanitization issue is the SSE line framing issue above, which is not an HTML escaping problem but can still affect browser-facing clients.
