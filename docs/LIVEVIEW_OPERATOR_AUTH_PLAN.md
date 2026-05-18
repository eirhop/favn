# LiveView Operator Authentication Plan

## Executive Recommendation

Add a boring Phoenix LiveView auth boundary in `favn_view` that stores only a random browser session id and `live_socket_id` in the Phoenix session, keeps the raw orchestrator session token server-side, resolves a `%FavnView.Auth.Scope{}` through public `FavnOrchestrator` facade calls, protects all operator LiveViews with a named `live_session` and `on_mount`, and performs server-side role checks before every mutating `handle_event`.

Proposed PR title: `Protect LiveView operator routes with orchestrator-backed auth`

This is production-critical. The current LiveView browser surface exposes operator pages and run/backfill submission paths without a browser login boundary or visible server-side role checks.

## Current-State Findings

`apps/favn_view/lib/favn_view/router.ex` defines the standard browser pipeline: `accepts ["html"]`, `fetch_session`, `fetch_live_flash`, `put_root_layout`, `protect_from_forgery`, and `put_secure_browser_headers`.

`apps/favn_view/lib/favn_view/endpoint.ex` uses a cookie session with key `_favn_view_key`, signing salt, encryption salt, explicit `http_only: true`, `same_site: "Lax"`, and production `secure: true`. The LiveView socket receives session data through `connect_info: [session: @session_options]`. The cookie must not store actor details, roles, credentials, raw tokens, or diagnostics.

Unauthenticated health/readiness routes exist and must remain public:

- `GET /api/web/v1/health/live`
- `GET /api/web/v1/health/ready`

Current operator routes are all exposed under the browser pipeline without a protected `live_session`:

- `/`
- `/assets`
- `/assets/:asset_id`
- `/pipelines`
- `/pipelines/:pipeline_id`
- `/runs`
- `/runs/:run_id`
- `/runs/:run_id/logs`
- `/runs/:run_id/assets/:asset_step_id/logs`
- `/logs`

There is no current `/login` or `/logout` browser flow in `favn_view`.

Current mutating LiveView events that must be guarded:

- `FavnView.PipelineDetailLive.handle_event("run_pipeline", ...)` calls `FavnOrchestrator.submit_pipeline_run_for_manifest/3`.
- `FavnView.PipelineDetailLive.handle_event("submit_backfill", ...)` calls `FavnOrchestrator.submit_pipeline_backfill_for_manifest/3`.
- `FavnView.AssetDetailLive.handle_event("run_selected_window", ...)` calls `FavnOrchestrator.submit_asset_run_for_manifest/3`.

Current read/filter/toggle events can remain viewer-authorized page interactions, but they must run only after authenticated mount.

Static inspection found `favn_view` calling public `FavnOrchestrator` facade functions for operator data. It did not find direct `favn_view` calls to `FavnOrchestrator.Storage`, scheduler internals, runner internals, repos, or auth storage internals.

The top-level `FavnOrchestrator` facade exposes operator read/write functions, but not browser-auth facade functions. Auth helpers exist under `FavnOrchestrator.Auth`: password login, session introspection, session revocation, role checks, actor management, and audit logging. The orchestrator HTTP API already protects non-health routes with service auth plus actor context and role checks for `:viewer`, `:operator`, and `:admin`.

Existing relevant tests:

- `apps/favn_view/test/favn_view/web_readiness_test.exs` covers unauthenticated web health/readiness, readiness redaction, public-origin endpoint config, and public-facade usage for readiness.
- `apps/favn_view/test/favn_view/page_live_test.exs` covers current unauthenticated LiveView page loads and run/backfill submissions.
- `apps/favn_orchestrator/test/auth/store_test.exs` covers actor/session/audit persistence, password policy, invalid credential behavior, password-change session revocation, session TTL, and revoked session invalidation.
- `apps/favn_orchestrator/test/api/router_test.exs` covers orchestrator health without auth and protected API behavior using service tokens and actor roles.

Gaps to close:

- no browser login/logout flow in `favn_view`
- no protected LiveView session boundary
- no plug-level authentication for protected browser requests
- no LiveView `on_mount` authentication/authorization
- no current scope assigned to `conn` or `socket`
- no server-side role checks in mutating LiveView events
- no LiveView socket disconnect on logout/session revocation
- no `favn_view` tests for unauthenticated redirects, login, logout, revocation, or role behavior
- no top-level public `FavnOrchestrator` facade functions designed for same-BEAM browser auth glue

## Proposed Module, Route, And Session Design

Add `FavnView.Auth` as the thin browser-auth glue module. It owns Phoenix plugs, LiveView `on_mount` hooks, session cookie updates, redirects, flash behavior, and conversion from orchestrator auth results into view-local assigns. It must not own password hashing, durable sessions, actor storage, role persistence, audit storage, service-token internals, or raw auth storage calls.

Add `FavnView.Auth.Scope` as a small view-local struct containing sanitized `actor`, sanitized `session`, and normalized `roles`. Do not assign raw session tokens, password material, token hashes, credentials, service tokens, or sensitive diagnostics to `conn`, `socket`, templates, logs, flashes, or telemetry metadata.

Add public same-BEAM facade functions to `FavnOrchestrator` instead of calling `FavnOrchestrator.Auth` directly from `favn_view`. In simple terms: `favn_view` should ask the orchestrator, "is this browser operator logged in and allowed to do this?" through the same public front door it already uses for runtime data. It should not reach into the orchestrator's auth internals just because both apps run in the same BEAM.

- `operator_password_login(username, password)` returning `{:ok, session, actor}` or sanitized errors.
- `introspect_operator_session(session_token)` returning `{:ok, session, actor}` or `{:error, :invalid_session}`.
- `revoke_operator_session(session_id)` returning `:ok | {:error, term()}`.
- `operator_has_role?(actor, role)` returning `boolean()`.
- Mutating operator command variants that require actor context, such as `submit_operator_pipeline_run(scope, manifest_version_id, target_id, opts)`, or existing mutating facade functions extended with `actor_context: %{actor_id: ..., session_id: ..., required_role: :operator}`. Recommendation: add explicit operator command wrappers first, keep existing unauthenticated runtime helpers for internal/runtime use, and make LiveView call only the operator wrappers.

Phoenix session should store only:

- `:operator_browser_session_id`, a random id for the server-side browser session mapping
- `:live_socket_id`, for example `"operator_browser_sessions:#{browser_session_id}"`
- optional `:operator_return_to`, only for safe local paths during redirects

The raw orchestrator session token should stay server-side in a volatile browser-session mapping for the current single-BEAM production target. It must not be stored in the signed Phoenix cookie session because signed cookies are client-readable unless encrypted.

`conn.assigns` and `socket.assigns` should store:

- `:current_scope`, a `%FavnView.Auth.Scope{}`
- optional sanitized `:current_actor`
- derived booleans like `:can_submit_runs?`, only as rendering hints

LiveView reconnect and revocation should use `live_socket_id`:

- login puts `:live_socket_id` into the Phoenix session as `"operator_browser_sessions:#{browser_session_id}"`
- logout revokes the orchestrator session, broadcasts `"disconnect"` to that topic, clears the browser session, and redirects to `/login`
- external session revocation should broadcast the same topic when possible
- if a socket reconnects with a revoked/expired token, `on_mount` must halt and redirect to `/login`

Recommended router shape:

```elixir
scope "/", FavnView do
  pipe_through [:browser, :redirect_if_operator_authenticated]

  get "/login", OperatorSessionController, :new
  post "/login", OperatorSessionController, :create
end

scope "/", FavnView do
  pipe_through [:browser, :require_operator_authenticated]

  delete "/logout", OperatorSessionController, :delete

  live_session :operator,
    on_mount: [{FavnView.Auth, :require_authenticated_operator}] do
    live "/", PageLive, :home
    live "/assets", AssetCatalogueLive, :index
    live "/assets/:asset_id", AssetDetailLive, :show
    live "/pipelines", PipelinesLive, :index
    live "/pipelines/:pipeline_id", PipelineDetailLive, :show
    live "/runs", RunsListLive, :index
    live "/runs/:run_id", RunDetailLive, :show
    live "/runs/:run_id/logs", RunLogsLive, :show
    live "/runs/:run_id/assets/:asset_step_id/logs", AssetRunLogsLive, :show
    live "/logs", LogsLive, :index
  end
end
```

Prefer `DELETE /logout` for normal UI logout. A `POST /logout` fallback is acceptable if the component stack makes `DELETE` awkward, but do not introduce state-changing `GET` routes.

`/` should remain an authenticated home route and redirect to `/assets` after auth. Unauthenticated `/` should redirect to `/login?return_to=/` through the plug layer.

## Login And Logout Behavior

The login form should be a plain Phoenix controller-backed form with fields:

- `username`
- `password`
- optional hidden `return_to` containing a validated local path

Failed login must use a generic message such as `Invalid username or password`, preserve the username field only if useful, clear the password field, return 401 or render with an error status, and avoid user enumeration details.

Successful login should call the public orchestrator facade, put the raw orchestrator session token into the server-side browser-session mapping, renew the Phoenix session, put only `:operator_browser_session_id` and `:live_socket_id`, optionally write safe `return_to`, and redirect to the intended local path or `/assets` with a concise flash.

Logout should introspect or read the current scope, revoke the orchestrator session through the public facade, best-effort broadcast `disconnect` to the `live_socket_id`, clear the Phoenix session, and redirect to `/login` with a generic signed-out flash.

Session revocation elsewhere should invalidate reconnects through orchestrator introspection. If the revoking path knows the browser session id, it should also broadcast disconnect to `operator_browser_sessions:<browser_session_id>`.

Redaction rules:

- never flash or log raw tokens, token hashes, passwords, service tokens, or credential errors
- do not show whether username or password failed
- map unexpected auth failures to generic user-facing messages and structured internal logs with redaction

## Authorization Matrix

| Capability | viewer | operator | admin |
| --- | --- | --- | --- |
| View `/assets`, asset detail | yes | yes | yes |
| View `/pipelines`, pipeline detail | yes | yes | yes |
| View `/runs`, run detail | yes | yes | yes |
| View `/logs`, run logs, asset-step logs | yes | yes | yes |
| Submit asset run | no | yes | yes |
| Submit pipeline run | no | yes | yes |
| Submit pipeline backfill | no | yes | yes |
| Cancel, retry, or rerun runs if added to LiveView | no | yes | yes |
| Actor, audit, and admin UI later | no | no | yes |

Page-level authorization should run in `FavnView.Auth.on_mount/4`. For this set of routes, `:viewer` is sufficient to mount. If future pages require admin access, put them in a separate `live_session :operator_admin` with an admin `on_mount`, or keep the same live session and use explicit per-page authorization metadata.

Action-level authorization must run inside each mutating `handle_event` before calling the orchestrator command. UI hiding is only a usability hint. A forged LiveView event from a viewer must fail server-side.

The orchestrator facade should also enforce role checks for mutating commands as defense-in-depth. The HTTP API already does this for run submit, cancel, rerun, backfill submit, repair, and admin endpoints; same-BEAM facade calls used by LiveView need equivalent actor-context enforcement.

## Boundary Rules

`favn_view` must not directly call auth storage, SQLite repos, password hashing internals, audit storage, scheduler internals, runner internals, storage adapters, compiler internals, plugin internals, or service-token internals.

`favn_view` may call public `FavnOrchestrator` facade functions. Auth/session/audit state remains orchestrator-owned.

Do not expose raw tokens, credential material, token hashes, or sensitive diagnostics in assigns, logs, flashes, templates, tests, or error responses.

## Security Controls

Preserve CSRF protection and secure browser headers in the browser pipeline.

Preserve `same_site: "Lax"` session behavior. Consider encrypted cookie sessions only if future code must store readable-sensitive browser state, but the preferred design is not to store such state.

Keep websocket origin checks tied to `FAVN_VIEW_PUBLIC_ORIGIN`; `FavnView.ProductionRuntimeConfig` currently writes endpoint `:check_origin` from that origin.

Keep production force-SSL behavior intact; `config/prod.exs` configures `force_ssl` with `rewrite_on: [:x_forwarded_proto]` and HSTS behavior.

Do not introduce state-changing HTTP GET routes.

Treat LiveView events and params as untrusted client input. Re-validate action permissions and input shape on the server before every mutation.

Add login rate limiting or backoff before exposing browser login in production. Recommendation: enforce it in the orchestrator auth facade, keyed by normalized username plus remote IP where available, with generic failed-login responses and redacted audit entries. `favn_view` may add a small browser-facing delay or generic flash, but durable counters and policy should remain orchestrator-owned.

Sanitize sensitive errors before rendering, flashing, or logging.

## File-By-File Implementation Plan

`apps/favn_orchestrator/lib/favn_orchestrator.ex`:

- Add public auth facade functions with docs, specs, and sanitized return shapes.
- Add explicit operator mutating command wrappers that require actor/session context and enforce `:operator` role before delegating to existing runtime helpers.
- Ensure operator-required commands audit through orchestrator-owned audit facilities.
- Add orchestrator-owned login rate limiting/backoff for password login attempts.

`apps/favn_orchestrator/test/*`:

- Add facade tests for login, introspection, revocation, role checks, and mutating command authorization.

`apps/favn_view/lib/favn_view/auth.ex`:

- Add plugs for fetching current scope, requiring authenticated operators, redirecting authenticated operators away from login, logging in, logging out, and broadcasting disconnects.
- Add `on_mount` hooks for authenticated operator scope and optional role-specific hooks.

`apps/favn_view/lib/favn_view/auth/scope.ex`:

- Define the sanitized scope struct, types, and constructors from orchestrator actor/session data.

`apps/favn_view/lib/favn_view/controllers/operator_session_controller.ex`:

- Add `new`, `create`, and `delete` actions for login/logout.

`apps/favn_view/lib/favn_view/router.ex`:

- Add public `/login` routes.
- Add protected `/logout` route.
- Move operator LiveViews into `live_session :operator` with auth `on_mount` and a matching protected browser plug.
- Keep health/readiness unauthenticated.

`apps/favn_view/lib/favn_view/pipeline_detail_live.ex`:

- Guard `run_pipeline` and `submit_backfill` with `operator` permission before command calls.
- Pass actor/session context into the orchestrator facade.

`apps/favn_view/lib/favn_view/asset_detail_live.ex`:

- Guard `run_selected_window` with `operator` permission before command calls.
- Pass actor/session context into the orchestrator facade.

`apps/favn_view/lib/favn_view/components/*`:

- Hide/disable run/backfill controls for viewers as a usability hint only.
- Avoid leaking authorization details or sensitive auth state.

`apps/favn_view/test/*`:

- Add ConnCase/LiveView helpers for authenticated viewer/operator/admin sessions using orchestrator-owned auth setup.
- Update existing page tests to authenticate explicitly.

Docs:

- Update `docs/production/single_node_operator_runbook.md` with operator login/logout behavior.
- Update `docs/production/single_node_contract.md` to clarify the browser auth boundary.
- Update `docs/FEATURES.md` after implementation to reflect UI/security state.
- Update `docs/structure/favn_view.md` to document that auth glue lives in view but durable auth belongs to orchestrator.

## Ordered Implementation Phases

1. Inventory and facade decisions: add public `FavnOrchestrator` operator auth functions and explicit operator command wrappers for LiveView mutations.
2. Add `FavnView.Auth` and scope/session plumbing: plugs, `on_mount`, scope struct, session renewal, and socket disconnect helper.
3. Add login/logout UI: controller, template/component, redirects, flashes, and redaction behavior.
4. Protect routes: split public and protected browser scopes, add `live_session :operator`, and keep health/readiness public.
5. Add role checks to mutating LiveView events: asset run, pipeline run, pipeline backfill, and any future cancel/retry/rerun paths.
6. Add tests and docs: update existing LiveView tests to authenticate and add focused auth/security coverage.
7. Optional browser smoke after merge: login, authenticated page load, and one minimal operator page render.

## Test Plan

Router and controller tests:

- `/login` renders without auth.
- authenticated operator visiting `/login` redirects to `/assets` or `return_to`.
- unauthenticated `/`, `/assets`, `/pipelines`, `/runs`, `/logs`, and detail routes redirect to `/login`.
- health/readiness remain unauthenticated.
- logout revokes the orchestrator session, clears the Phoenix session, and redirects to `/login`.
- logout disconnects LiveView sockets through `live_socket_id` where testable.

LiveView tests:

- unauthenticated protected LiveView mount redirects.
- viewer can mount and read assets, pipelines, runs, and logs.
- viewer cannot submit asset runs, pipeline runs, or backfills by forged `render_submit`/event.
- operator can submit asset runs, pipeline runs, and backfills.
- revoked/expired sessions fail mount and redirect.

Boundary tests:

- `favn_view` auth code does not reference `FavnOrchestrator.Auth.Store`, `FavnOrchestrator.Storage`, storage adapters, scheduler internals, runner internals, password hashing modules, or service-token internals.
- session assigns and rendered HTML do not contain raw session tokens or credential material.

Orchestrator facade tests:

- password login success/failure through the public facade.
- login rate limiting/backoff rejects repeated failures without revealing whether username or password was wrong.
- session introspection success/failure through the public facade.
- revocation invalidates introspection.
- role hierarchy treats admin as operator/viewer and operator as viewer.
- mutating same-BEAM facade commands reject viewer actor context and accept operator/admin context.

Optional browser smoke follow-up:

- login -> `/assets` -> navigate to one operator page -> logout -> protected route redirects to `/login`.

## Verification Commands

Run focused checks first:

```bash
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser
```

Before finishing implementation, run:

```bash
mix format
mix compile --warnings-as-errors
mix test
```

If browser smoke is added later, use the repo's browser-tagged slice or `mix test.acceptance` only after the focused tests pass.

## Resolved Decisions And Follow-Ups

- Promote operator auth functions to the top-level `FavnOrchestrator` facade for same-BEAM LiveView use. This is the production-safe boundary: `favn_view` uses one public orchestrator front door and does not call auth internals.
- Add explicit operator command wrappers for LiveView mutations. Existing unauthenticated runtime helpers may remain for internal/runtime use, but LiveView must call wrappers that require actor/session context and enforce `:operator`.
- Add login rate limiting/backoff before production browser login ships. Keep the durable policy in orchestrator-owned auth, not in `favn_view`.
- Should session revocation disconnect by session id only, actor id only, or both? Recommendation: session id now, actor id later for admin-wide actor lockout.
- `viewer` can see logs and run details for current operator pages. Revisit before exposing broader raw debug payloads.
- Defer actor/admin UI because no admin UI exists yet. Protect the operator surface first.
- No hidden LiveView cancel/retry/rerun events were found in static inspection. If future LiveView cancel, retry, rerun, repair, or admin events are added, treat them as `operator` or `admin` mutations and test forged events.

Create follow-up issues for important deferred security work:

- actor-wide LiveView disconnect/admin lockout by actor id
- admin UI design for actor/session/audit management
- broader raw debug/log authorization review before adding more sensitive diagnostics
