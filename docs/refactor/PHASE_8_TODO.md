# Phase 8 TODO

## Status

Checklist for implementing `docs/refactor/PHASE_8_WEB_ORCHESTRATOR_BOUNDARY_PLAN.md`.

This list is intentionally detailed and execution-oriented. `docs/FEATURES.md` remains the high-level roadmap only.

## Architecture Correction And Doc Realignment

- [x] Replace the old same-BEAM Phase 8 plan with the corrected `web + orchestrator + runner` Phase 8 brief.
- [x] Update roadmap/docs summaries so `favn_view` is no longer documented as the steady-state target.
- [x] Reframe Phase 9 as tooling and packaging around `web`, `orchestrator`, `runner`, and optional `single`.

## Orchestrator Auth Domain

- [x] Add orchestrator-owned `actors`, `actor_roles`, `auth_identities`, `password_credentials`, `sessions`, and `audit_entries` foundations.
- [x] Add provider abstraction with local-password provider as the default `v0.5` implementation.
- [x] Add admin bootstrap path for first user creation.
- [x] Add password set/reset flows for admin-managed users.

## Orchestrator HTTP API Foundation

- [x] Add versioned private orchestrator HTTP API namespace (`/api/orchestrator/v1`).
- [x] Add shared success/error envelope conventions and request-id propagation.
- [x] Add machine-readable schema ownership for remote DTOs under orchestrator.
- [x] Add contract-lock tests for request/response shapes.

## Auth And Session Endpoints

- [x] Add `POST /auth/password/sessions` for local password login.
- [x] Add `POST /auth/sessions/introspect` for trusted web-session lookup.
- [x] Add `POST /auth/sessions/:session_id/revoke` for logout/session revocation.
- [x] Add `GET /me` for current actor/session introspection.

## Read APIs

- [x] Add manifests list/detail and active manifest endpoints.
- [x] Add runs list/detail/event-history endpoints.
- [x] Add schedules list/detail inspection endpoints.
- [x] Add actors/audit read endpoints needed for admin/operator views.

## Mutating Operator Commands

- [x] Add run submit endpoint with required idempotency handling.
- [x] Add cancel and rerun command endpoints with required idempotency handling.
- [x] Persist and replay successful command responses for repeated `Idempotency-Key` requests within the configured TTL window.
- [x] Add manifest activation endpoint with authz and audit coverage.
- [x] Add consistent command authorization and audit hooks.

## Service-To-Service Trust

- [x] Add explicit web-to-orchestrator service authentication.
- [x] Reject requests missing valid service credentials even on private networks.
- [x] Fail closed when orchestrator API service tokens are not explicitly configured (test-only default allowed).
- [x] Validate forwarded actor/session context in orchestrator on every request.
- [x] Add audit attribution for service identity plus actor identity.

## SSE Event Streams

- [x] Add private orchestrator SSE endpoints for global runs and run-scoped streams.
- [x] Add opaque cursor ids and `Last-Event-ID` resume handling.
- [x] Reject malformed `Last-Event-ID` headers at both web relay and orchestrator boundaries.
- [x] Keep orchestrator as the authoritative replay source from persisted history.
- [x] Add stream authz checks for viewer/operator/admin roles.

## Thin `favn_web` Prototype

- [x] Create a separate web workspace/package outside the Elixir umbrella.
- [x] Add browser login/logout flows with secure HttpOnly cookie sessions.
- [x] Sign web session cookies and reject tampered payloads before forwarding actor context.
- [x] Ensure browser auth failures are slowed/rate-limited to reduce brute-force pressure.
- [x] Add thin BFF routes for run/manifest/schedule pages.
- [x] Add browser-facing SSE endpoints that authenticate the cookie session and relay orchestrator streams.
- [x] Add run-scoped browser-facing SSE relay endpoint (`/api/web/v1/streams/runs/:run_id`).
- [x] Prove minimal operator flows over the remote boundary: run list/detail, submit, cancel, rerun, manifest list/detail, scheduler inspection.

## Transitional `favn_view` Handling

- [x] Freeze `favn_view` as transitional reference only.
- [x] Avoid landing new product-boundary semantics in `favn_view`.
- [x] Remove `favn_view` from active Phase 8 validation paths after `favn_web` smoke coverage landed (archived, no longer in umbrella `mix test` alias).

## Testing

- [x] Add HTTP contract tests for v1 endpoints.
- [x] Add auth/authz role-matrix tests.
- [x] Add password/session tests.
- [x] Add service-auth rejection tests.
- [x] Add SSE replay/resume tests.
- [x] Add thin web browser smoke tests.
- [x] Move `mix favn.dev` / `mix favn.dev --sqlite` smoke coverage tracking to Phase 9 tooling scope.

## Verification Gate (Per AGENTS.md)

- [x] Run `mix format` after Elixir implementation slices.
- [x] Run `mix compile --warnings-as-errors` after Elixir implementation slices.
- [x] Run `mix test` after Elixir implementation slices.
- [x] Run `mix credo --strict` after Elixir implementation slices.
- [x] Run `mix dialyzer` after Elixir implementation slices.
- [x] Run `mix xref graph --format stats --label compile-connected` after Elixir implementation slices.

## Explicit Non-Goals For This Phase

- [ ] same-BEAM `favn_view -> favn_orchestrator` as the steady-state product path.
- [ ] frontend-driven backend contract design.
- [ ] public runner exposure.
- [ ] browser cookies as the orchestrator trust boundary.
- [ ] WebSocket-first live update design.
- [ ] Entra ID as the default first provider.
