# Phase 8 TODO

## Status

Checklist for implementing `docs/refactor/PHASE_8_WEB_ORCHESTRATOR_BOUNDARY_PLAN.md`.

This list is intentionally detailed and execution-oriented. `docs/FEATURES.md` remains the high-level roadmap only.

## Architecture Correction And Doc Realignment

- [x] Replace the old same-BEAM Phase 8 plan with the corrected `web + orchestrator + runner` Phase 8 brief.
- [x] Update roadmap/docs summaries so `favn_view` is no longer documented as the steady-state target.
- [x] Reframe Phase 9 as tooling and packaging around `web`, `orchestrator`, `runner`, and optional `single`.

## Orchestrator Auth Domain

- [ ] Add orchestrator-owned `actors`, `actor_roles`, `auth_identities`, `password_credentials`, `sessions`, and `audit_entries` foundations.
- [ ] Add provider abstraction with local-password provider as the default `v0.5` implementation.
- [ ] Add admin bootstrap path for first user creation.
- [ ] Add password set/reset flows for admin-managed users.

## Orchestrator HTTP API Foundation

- [ ] Add versioned private orchestrator HTTP API namespace (`/api/orchestrator/v1`).
- [ ] Add shared success/error envelope conventions and request-id propagation.
- [ ] Add machine-readable schema ownership for remote DTOs under orchestrator.
- [ ] Add contract-lock tests for request/response shapes.

## Auth And Session Endpoints

- [ ] Add `POST /auth/password/sessions` for local password login.
- [ ] Add `POST /auth/sessions/introspect` for trusted web-session lookup.
- [ ] Add `POST /auth/sessions/:session_id/revoke` for logout/session revocation.
- [ ] Add `GET /me` for current actor/session introspection.

## Read APIs

- [ ] Add manifests list/detail and active manifest endpoints.
- [ ] Add runs list/detail/event-history endpoints.
- [ ] Add schedules list/detail inspection endpoints.
- [ ] Add actors/audit read endpoints needed for admin/operator views.

## Mutating Operator Commands

- [ ] Add run submit endpoint with required idempotency handling.
- [ ] Add cancel and rerun command endpoints with required idempotency handling.
- [ ] Add manifest activation endpoint with authz and audit coverage.
- [ ] Add consistent command authorization and audit hooks.

## Service-To-Service Trust

- [ ] Add explicit web-to-orchestrator service authentication.
- [ ] Reject requests missing valid service credentials even on private networks.
- [ ] Validate forwarded actor/session context in orchestrator on every request.
- [ ] Add audit attribution for service identity plus actor identity.

## SSE Event Streams

- [ ] Add private orchestrator SSE endpoints for global runs and run-scoped streams.
- [ ] Add opaque cursor ids and `Last-Event-ID` resume handling.
- [ ] Keep orchestrator as the authoritative replay source from persisted history.
- [ ] Add stream authz checks for viewer/operator/admin roles.

## Thin `favn_web` Prototype

- [ ] Create a separate web workspace/package outside the Elixir umbrella.
- [ ] Add browser login/logout flows with secure HttpOnly cookie sessions.
- [ ] Add thin BFF routes for run/manifest/schedule pages.
- [ ] Add browser-facing SSE endpoints that authenticate the cookie session and relay orchestrator streams.
- [ ] Prove minimal operator flows over the remote boundary: run list/detail, submit, cancel, rerun, manifest list/detail, scheduler inspection.

## Transitional `favn_view` Handling

- [ ] Freeze `favn_view` as transitional reference only.
- [ ] Avoid landing new product-boundary semantics in `favn_view`.
- [ ] Remove `favn_view` after `favn_web` covers the required smoke flows.

## Testing

- [ ] Add HTTP contract tests for v1 endpoints.
- [ ] Add auth/authz role-matrix tests.
- [ ] Add password/session tests.
- [ ] Add service-auth rejection tests.
- [ ] Add SSE replay/resume tests.
- [ ] Add thin web browser smoke tests.
- [ ] Add local-dev smoke coverage for `mix favn.dev` and `mix favn.dev --sqlite`.

## Verification Gate (Per AGENTS.md)

- [ ] Run `mix format` after Elixir implementation slices.
- [ ] Run `mix compile --warnings-as-errors` after Elixir implementation slices.
- [ ] Run `mix test` after Elixir implementation slices.
- [ ] Run `mix credo --strict` after Elixir implementation slices.
- [ ] Run `mix dialyzer` after Elixir implementation slices.
- [ ] Run `mix xref graph --format stats --label compile-connected` after Elixir implementation slices.

## Explicit Non-Goals For This Phase

- [ ] same-BEAM `favn_view -> favn_orchestrator` as the steady-state product path.
- [ ] frontend-driven backend contract design.
- [ ] public runner exposure.
- [ ] browser cookies as the orchestrator trust boundary.
- [ ] WebSocket-first live update design.
- [ ] Entra ID as the default first provider.
