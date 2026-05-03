# Phase 8 Web/Orchestrator Boundary, Event, and Auth Plan

## Status

Reopened and redefined.

This plan replaces the earlier same-BEAM `favn_view -> favn_orchestrator` LiveView prototype as the Phase 8 target definition. The existing `favn_view` code remains a transitional prototype only. It is no longer the steady-state architecture to optimize around.

## 1. Restatement of the New Architecture

Favn is now targeting a three-tier runtime shape:

- `favn_web`: public web/BFF tier, likely SvelteKit, owning browser UI, login/logout, cookie sessions, browser-facing HTTP and SSE, request shaping, and rate limiting.
- `favn_orchestrator`: private control plane, owning manifests, active manifest selection, runs, run events, scheduler cursors, admission, planning, retries/timeouts/cancellation, actor records, authorization, audit, and local password auth for v0.5.
- `favn_runner`: private execution tier, owning manifest-backed work execution, runtime value resolution, and result normalization only.

Locked consequences for current Phase 8 work:

- same-BEAM `favn_view -> favn_orchestrator` calls are now transitional, not the long-term boundary.
- Phoenix LiveView is no longer the product-shaping UI assumption.
- the durable asset for v0.5 is the orchestrator HTTP API, SSE contract, auth/authz model, and audit/event model.
- browser clients must never connect directly to orchestrator or runners.
- orchestrator stays private by default.

## 2. Current Doc/Roadmap Mismatches

The current repo docs still encode the old Phase 8 direction and must be corrected.

### `README.md`

Current mismatches:

- states "Phase 8 `favn_view` + orchestrator live-event boundary is complete".
- says current implementation focus is Phase 9 tooling/packaging without first correcting the web/orchestrator boundary.
- points to `docs/refactor/PHASE_8_VIEW_PROTOTYPE_PLAN.md`, which encodes the wrong steady-state target.
- presents `favn_view` runtime config without clarifying that it is transitional.

Required change:

- restate current focus as corrected Phase 8 preparation for Phase 9.
- mark `favn_view` as a disposable prototype.
- point to this Phase 8 plan as the source of truth.

### `docs/REFACTOR.md`

Current mismatches:

- defines the product split as `favn` + `favn_runner` + `favn_orchestrator` + `favn_view`.
- encodes `favn_view` as a locked steady-state runtime app.
- locks `favn_view -> favn_orchestrator` as an allowed dependency direction.
- treats Phoenix/LiveView UI as the Phase 8 deliverable.
- treats Phase 8 as implemented and Phase 9 as ready to proceed on top of that boundary.
- assumes split deployment as `view + orchestrator + runner`, not `web + orchestrator + runner`.

Required change:

- redefine the steady-state architecture as `web + orchestrator + runner`.
- explicitly demote `favn_view` to transitional prototype status.
- reopen Phase 8 around remote API, SSE, auth/authz, and audit preparation.
- reframe Phase 9 around the honest deployment targets: `web`, `orchestrator`, `runner`, and optional `single` packaging.

### `docs/FEATURES.md`

Current mismatches:

- describes v0.5 as separate authoring, runner, orchestrator, and view boundaries.
- marks Phase 8 `favn_view` work as complete.
- lists the LiveView prototype as the accepted implementation result.
- treats Phase 9 as only tooling/package polish instead of architecture-following tooling.

Required change:

- mark the earlier `favn_view` work as transitional history, not the accepted end-state.
- redefine Phase 8 deliverables around remote contracts and auth.
- update Phase 9 to package the corrected topology.

### Current Phase 8 Prototype Docs

Current mismatches in `docs/refactor/PHASE_8_VIEW_PROTOTYPE_PLAN.md` and `docs/refactor/PHASE_8_TODO.md`:

- same-BEAM direct calls are treated as the primary v1 boundary.
- no HTTP product API is designed.
- auth/RBAC is declared out of scope.
- LiveView screens are treated as the main investment area.
- orchestrator PubSub topics are treated as the browser update contract.

Required change:

- replace them with a remote orchestrator API plan.
- treat SSE as the live-update contract.
- treat local username/password auth and orchestrator authz as locked v0.5 work.
- narrow the web prototype to proof-of-boundary only.

### Current Phase 9 Tooling/Packaging Assumptions

Current mismatches:

- local dev tooling assumes orchestrator + view runtime artifacts.
- packaging targets are framed around `favn_view`.
- the likely task surface does not account for a separate web workspace/package.
- current wording leaves the false impression that packaging can be finalized before the remote boundary and auth model are locked.

Required change:

- Phase 9 must package the corrected shape: `web`, `orchestrator`, `runner`, and maybe `single`.
- local dev must support a separate web dev/runtime process.
- hot-reload expectations must align with a separate JS web tier plus Elixir backend releases.

## 3. Proposed Redefinition of Phase 8

### Phase 8 goal

Establish the correct long-term remote boundary between `favn_web` and `favn_orchestrator`, including the canonical HTTP API, SSE event model, local-password auth foundation, service-to-service trust, and actor/audit semantics needed before Phase 9 tooling and packaging are finalized.

Scope control rule:

- Phase 8 establishes boundary correctness.
- Phase 8 does not claim safe-release security/scalability hardening.
- durable hardening required before a safe web-facing release is explicitly deferred to roadmap release blockers.

### Phase 8 priority order

1. orchestrator API design and implementation
2. authoritative live-event contract and SSE transport
3. orchestrator-owned auth/authz and audit model
4. minimal web prototype proving the boundary

### Phase 8 scope

In scope:

- private orchestrator HTTP API v1
- orchestrator-owned remote DTOs and error envelopes
- orchestrator-owned SSE endpoints with transport baseline and run-scoped replay/resume foundation
- local username/password auth backend in orchestrator
- actor, role, session, and audit record model in orchestrator
- explicit service-to-service auth from web to orchestrator
- thin browser-facing web endpoints and SSE relay behavior
- minimal browser flows: login, logout, current session, runs list/detail, manifests list/detail, scheduler inspection, submit/cancel/rerun

Out of scope:

- treating `favn_view` as the long-term product UI
- rich frontend polish or design-system investment
- WebSocket transport
- direct browser access to orchestrator
- pushing orchestration policy into the web tier
- Azure Entra ID as the default provider for v0.5

### Phase 8 deliverables

- a canonical orchestrator API contract document and HTTP endpoint skeleton
- a canonical SSE event contract document and baseline transport semantics
- actor/auth/session/audit domain model locked in orchestrator
- a thin `favn_web` prototype proving login, session handling, reads, commands, and SSE relay
- `favn_view` explicitly frozen as transitional
- Phase 9 reframed around the corrected deployment shape

Not a Phase 8 deliverable:

- release-grade login abuse controls
- durable idempotency replay contract
- scalable global SSE replay/cursor model

## 4. Canonical Orchestrator API Design

### Boundary posture

- all browser-facing traffic goes to `favn_web`.
- `favn_web` talks to `favn_orchestrator` over a private authenticated HTTP boundary.
- the orchestrator API should be stable enough for later gateway exposure, but v0.5 treats it as a private backend API first.
- browser-specific DTO shaping belongs in `favn_web`; product-state ownership belongs in `favn_orchestrator`.

### Versioning strategy

- path versioning: `/api/orchestrator/v1/...`
- event envelope versioning: `schema_version`
- additive fields remain in `v1`
- breaking semantic or field-shape changes require `v2`

### Remote DTO ownership

- remote HTTP and SSE contracts belong to `favn_orchestrator`, not `favn_core`.
- recommended location: `apps/favn_orchestrator/priv/http_contract/` for machine-readable schemas plus matching contract tests in `apps/favn_orchestrator/test/http_contract/`.
- `favn_web` should generate or validate its local TS DTOs from orchestrator-owned schemas instead of sharing BEAM structs.

### Resource model

Core remote resources:

- manifest versions
- active manifest selection
- runs
- run events
- schedules and scheduler cursor inspection
- actors
- sessions
- audit entries
- auth providers

### Command model

Use resource-oriented reads plus explicit command endpoints for lifecycle changes.

Recommended command endpoints:

- `POST /api/orchestrator/v1/runs`
- `POST /api/orchestrator/v1/runs/:run_id/cancel`
- `POST /api/orchestrator/v1/runs/:run_id/rerun`
- `POST /api/orchestrator/v1/manifests/:manifest_version_id/activate`
- `POST /api/orchestrator/v1/auth/password/sessions`
- `POST /api/orchestrator/v1/auth/sessions/introspect`
- `POST /api/orchestrator/v1/auth/sessions/:session_id/revoke`
- `POST /api/orchestrator/v1/actors`
- `PUT /api/orchestrator/v1/actors/:actor_id/roles`
- `PUT /api/orchestrator/v1/actors/:actor_id/password`

### Read/snapshot model

Recommended read endpoints:

- `GET /api/orchestrator/v1/manifests`
- `GET /api/orchestrator/v1/manifests/:manifest_version_id`
- `GET /api/orchestrator/v1/manifests/active`
- `GET /api/orchestrator/v1/runs`
- `GET /api/orchestrator/v1/runs/:run_id`
- `GET /api/orchestrator/v1/runs/:run_id/events`
- `GET /api/orchestrator/v1/schedules`
- `GET /api/orchestrator/v1/schedules/:schedule_id`
- `GET /api/orchestrator/v1/me`
- `GET /api/orchestrator/v1/audit`
- `GET /api/orchestrator/v1/actors`
- `GET /api/orchestrator/v1/actors/:actor_id`

### Suggested DTOs

Recommended snapshot DTO families:

- `ManifestVersionSummary`
- `ManifestVersionDetail`
- `ActiveManifest`
- `RunSummary`
- `RunDetail`
- `RunEventRecord`
- `ScheduleSummary`
- `ScheduleDetail`
- `ActorSummary`
- `ActorDetail`
- `SessionInfo`
- `AuditEntry`

The orchestrator should expose stable JSON DTOs. It should not expose raw Ecto schemas, internal BEAM structs, or browser-only presenter maps.

### Example run submission contract

Recommended request:

```json
{
  "target": {
    "type": "asset",
    "id": "sales.daily_orders"
  },
  "manifest_selection": {
    "mode": "active"
  },
  "params": {},
  "reason": "manual operator trigger"
}
```

Recommended response:

```json
{
  "data": {
    "run": {
      "id": "run_01H...",
      "status": "queued",
      "target": {
        "type": "asset",
        "id": "sales.daily_orders"
      },
      "manifest_version_id": "mvr_01H...",
      "submitted_at": "2026-04-17T10:15:00Z",
      "submitted_by": {
        "actor_id": "act_01H...",
        "username": "alice"
      }
    }
  }
}
```

### Auth endpoints for local username/password flow

Recommended private orchestrator endpoints used by `favn_web`:

- `POST /api/orchestrator/v1/auth/password/sessions`
  - verifies username/password against orchestrator-owned credentials
  - creates an orchestrator session record
  - returns `session` + `actor` summary
- `POST /api/orchestrator/v1/auth/sessions/introspect`
  - validates a forwarded session id and actor id
  - returns active actor/session information for the web tier
- `POST /api/orchestrator/v1/auth/sessions/:session_id/revoke`
  - revokes the orchestrator session on logout or admin action
- `GET /api/orchestrator/v1/me`
  - returns current actor + role/session summary under forwarded trusted context

Recommended login response shape:

```json
{
  "data": {
    "session": {
      "id": "ses_01H...",
      "actor_id": "act_01H...",
      "provider": "password_local",
      "issued_at": "2026-04-17T10:15:00Z",
      "expires_at": "2026-04-17T22:15:00Z"
    },
    "actor": {
      "id": "act_01H...",
      "username": "alice",
      "display_name": "Alice Example",
      "roles": ["operator"],
      "status": "active"
    }
  }
}
```

### Error envelope

Recommended error envelope for all HTTP APIs:

```json
{
  "error": {
    "code": "run_conflict",
    "message": "Run is already terminal",
    "status": 409,
    "request_id": "req_01H...",
    "retryable": false,
    "details": {
      "run_id": "run_01H..."
    }
  }
}
```

Recommended error codes include:

- `validation_failed`
- `unauthenticated`
- `forbidden`
- `not_found`
- `conflict`
- `cursor_invalid`
- `service_unauthorized`

### Pagination and filtering

Recommended baseline:

- cursor pagination for `runs`, `audit`, and global event streams
- `after_sequence` + `limit` for `runs/:id/events`
- filter params for `runs`: `status`, `target_type`, `target_id`, `manifest_version_id`, `submitted_by`, `created_after`, `created_before`
- filter params for `audit`: `actor_id`, `action`, `resource_type`, `from`, `to`

### Idempotency

Phase 8 status:

- durable idempotency is deferred out of Phase 8
- mutating commands in Phase 8 should remain explicit baseline commands with authz/audit
- no claim of safe replay semantics should be made until idempotency is storage-backed and request-fingerprint aware

Required before safe web-facing release:

- persist idempotency records durably
- include request-fingerprint/body conflict detection
- define explicit behavior for key reuse conflicts

### Internal-only vs browser-safe exposure

Internal orchestrator endpoints that should stay service-only in v0.5:

- password login verification
- session introspection and revoke internals
- actor admin endpoints
- raw audit search
- manifest registration/upload flows used by tooling

Read and command endpoints that should be designed cleanly enough for later gateway exposure:

- manifest list/detail and active manifest read
- run list/detail/events
- run submit/cancel/rerun
- schedule read endpoints
- current actor endpoint

## 5. SSE / Live Event Model

### Transport rule

- use one-way SSE over HTTP
- do not introduce WebSocket unless a concrete bidirectional need appears later

### Orchestrator internal event stream endpoints

Recommended private endpoints:

- `GET /api/orchestrator/v1/streams/runs`
- `GET /api/orchestrator/v1/streams/runs/:run_id`
- later if needed: `GET /api/orchestrator/v1/streams/system`

Phase 8 minimum:

- run-global stream as baseline transport/ready channel (no scalable replay guarantees yet)
- run-scoped stream for run detail with replay/resume foundation
- scheduler pages may remain snapshot-driven in the first cut, but any later live scheduler updates should reuse the same event envelope and cursor model instead of inventing a second transport

### Browser-facing SSE endpoints in web

Recommended browser endpoints:

- `GET /api/web/v1/streams/runs`
- `GET /api/web/v1/streams/runs/:run_id`

`favn_web` responsibilities:

- authenticate the browser session cookie
- enforce browser-facing route permissions before opening the stream
- connect to the matching private orchestrator SSE endpoint using service auth plus forwarded trusted session context
- relay or lightly reshape safe event payloads
- never become the authority for history or replay

### Event envelope

Recommended generic envelope fields:

- `schema_version`
- `event_id`
- `stream`
- `topic`
- `event_type`
- `occurred_at`
- `actor`
- `resource`
- `sequence`
- `cursor`
- `data`

Recommended example:

```json
{
  "schema_version": 1,
  "event_id": "evt_01H...",
  "stream": "runs",
  "topic": {
    "type": "run",
    "id": "run_01H..."
  },
  "event_type": "run.step_succeeded",
  "occurred_at": "2026-04-17T10:16:03Z",
  "actor": {
    "type": "system",
    "id": "orchestrator"
  },
  "resource": {
    "type": "run",
    "id": "run_01H..."
  },
  "sequence": 14,
  "cursor": "runs:0000000000000142",
  "data": {
    "status": "running",
    "step": {
      "asset_ref": "sales.daily_orders"
    }
  }
}
```

SSE framing recommendation:

- SSE `id:` should be the opaque `cursor`
- SSE `event:` should be the string `event_type`
- SSE `data:` should be the JSON envelope

### Sequencing, replay, and cursor behavior

- run-scoped events keep a monotonic per-run `sequence`
- run-scoped SSE replay uses the opaque cursor plus per-run sequence semantics
- orchestrator remains the durable source of truth for run-scoped replay from persisted history
- global mixed-stream durable ordering/replay is deferred until a scalable cursor model is implemented

### Resume strategy

- browsers rely on `Last-Event-ID` automatically when reconnecting to `favn_web`
- `favn_web` forwards `Last-Event-ID` to orchestrator when reopening the upstream stream
- run-scoped streams replay events strictly after the supplied cursor
- global stream baseline may return a ready/reset state without claiming durable replay guarantees

### Reconnection model

- browser reconnects automatically via EventSource semantics
- web should not promise delivery while disconnected
- recovery is `snapshot read + replay from orchestrator`
- browser pages should treat snapshots as authoritative and events as incremental refresh hints

### Relay, filtering, and caching behavior in web

Recommended Phase 8 default:

- `favn_web` performs a thin authenticated relay per browser connection
- it may filter fields not intended for browser exposure
- it may lightly cache the latest snapshot or stream cursor for page bootstrapping
- it must not persist an authoritative event store or derive authoritative run state independently of orchestrator

## 6. Authentication/Authorization Model

### Ownership split

- browser authentication edge: `favn_web`
- user/account/session/provider/role/audit authority: `favn_orchestrator`
- execution-service trust: orchestrator and runner transport layer
- authorization decisions: `favn_orchestrator`

### Recommended boring default for v0.5

Use a Phoenix-native auth foundation inside `favn_orchestrator` for local password auth, adapted for service-facing HTTP endpoints rather than direct browser cookie auth.

Recommended core records:

- `actors`
- `actor_roles`
- `auth_identities`
- `password_credentials`
- `sessions`
- `audit_entries`

Recommended auth-provider model:

- provider id `password_local` for v0.5 default
- later provider id `entra_oidc`
- both map into the same stable `actors` and role/audit model

### Browser -> Web

Recommended flow:

1. browser posts username/password to `favn_web`
2. `favn_web` calls orchestrator password-session endpoint over service-authenticated private HTTP
3. orchestrator verifies password, creates the authoritative user session, and returns `session` + `actor`
4. `favn_web` stores a secure HttpOnly browser cookie carrying the minimum encrypted session payload needed to reconnect the browser to the orchestrator-backed session
5. browser never receives direct orchestrator credentials

Recommended browser cookie properties:

- `HttpOnly`
- `Secure`
- `SameSite=Lax` by default
- short idle timeout plus explicit max lifetime
- encrypted and signed by `favn_web`

Recommended cookie payload fields:

- orchestrator `session_id`
- `actor_id`
- provider id
- issued/expiry timestamps

Do not store roles or authorizations in the cookie as the source of truth.

### Web -> Orchestrator

Recommended boring default:

- private HTTPS
- static service bearer credential in config for v0.5
- every request includes service auth plus forwarded trusted session context headers

Recommended forwarded headers:

- `Authorization: Bearer <web-service-token>`
- `X-Favn-Actor-Id`
- `X-Favn-Session-Id`
- `X-Favn-Auth-Provider`
- `X-Favn-Request-Id`

Required orchestrator behavior:

- authenticate the service caller first
- validate the forwarded session against orchestrator-owned session records
- load current actor roles and permissions from orchestrator storage
- make the authorization decision in orchestrator
- write audit records in orchestrator for accepted and rejected operator actions as appropriate

The web tier may gate obvious UI routes, but it is never the authority for permission enforcement.

### Orchestrator -> Runner

Recommended v0.5 default:

- authenticate runner calls as service-to-service traffic
- do not forward browser cookies or treat runner as a user-authenticated surface
- keep user attribution as metadata for audit correlation only if needed
- keep runner auth transport-pluggable so HTTP, gRPC, or same-node development mode can all validate the orchestrator caller explicitly

The runner boundary remains execution-only. It does not own operator authz.

### User records, roles, and actor identities

Recommended v0.5 classes:

- `viewer`
- `operator`
- `admin`

Recommended permission model:

- `viewer`: read runs, manifests, schedules, and live streams
- `operator`: viewer permissions plus submit, cancel, rerun, and manifest activation
- `admin`: operator permissions plus actor/user management, password reset, session revoke, and full audit access

Store roles and permissions in orchestrator-owned tables, not in web-session state.

### Actor attribution and audit

Every mutating operator action should record at least:

- `actor_id`
- `session_id`
- `service_identity` (`favn_web`)
- action name
- resource type/id
- request id
- occurred-at timestamp
- outcome (`accepted`, `rejected`, `conflict`, `failed`)

Audit records are orchestrator-owned and append-only.

### Local dev auth story

Recommended default:

- keep the real password flow active in local dev
- add an explicit bootstrap task to create the first admin account
- allow a clearly marked dev-only seed path, but do not rely on hidden auth bypasses as the normal path
- use a fixed dev service token between web and orchestrator in dev config

### Future Azure Entra ID support

Planned extension path:

- `favn_web` handles the browser redirect/callback flow for OIDC
- orchestrator maps the provider subject to an existing or newly linked `actor`
- roles, authz, audit, and session semantics remain orchestrator-owned and unchanged
- adding Entra should add a provider implementation, not replace the actor/role model

### Future direct orchestrator API exposure

If later exposing orchestrator APIs via a gateway:

- do not reuse the internal web-service trust model directly for end-user traffic
- add a gateway-validated bearer token or orchestrator-native API token scheme
- keep the same resource DTOs, authz engine, and audit model

## 7. App/Repo Structure Implications

### `favn_web` placement

Recommended default:

- keep `favn_web` outside the Elixir umbrella as a separate monorepo workspace/package
- likely path: `web/favn_web/` or similar JS workspace root

Rationale:

- the boundary is remote HTTP/SSE, not same-BEAM calls
- separate tooling, dependency graph, and dev server behavior are easier to keep honest outside the umbrella
- this reduces the temptation to smuggle shared Elixir-only convenience modules across the boundary

### What happens to `favn_view`

Recommended default:

- deprecate and freeze `favn_view` as a short-lived prototype/reference
- do not evolve it as the product architecture
- keep it only long enough to preserve current operator workflow reference while `favn_web` proves the new boundary
- delete it once `favn_web` covers the required Phase 8/9 smoke flows

Do not repurpose `favn_view` into the long-term web tier.

### Shared API/event contracts

Recommended default:

- keep remote contracts owned by `favn_orchestrator`
- publish machine-readable schemas from orchestrator
- generate or validate matching TS types in `favn_web`
- do not move remote DTO ownership into `favn_core`

### Avoiding boundary violations

Recommended rules:

- no direct `favn_web` imports from umbrella Elixir apps
- no browser-specific presenter maps leaking back into orchestrator DTOs
- no same-BEAM shortcuts for reads or commands in production architecture paths
- no runner access from `favn_web`
- no storage-adapter access from `favn_web`

## 8. Phase 9 Implications

Phase 9 must now be reframed as tooling and packaging around the corrected three-tier topology.

### Honest target set

Recommended build/runtime targets:

- `web`
- `orchestrator`
- `runner`
- optional `single`

### Local dev tooling

Recommended direction:

- `mix favn.dev` starts orchestrator and local runner, and either starts or instructs startup of the separate web dev process
- `mix favn.dev --sqlite` wires SQLite for orchestrator persistence, not view-local storage
- local dev should make the private/public split visible even when everything runs on one machine

### Packaging/build targets

Recommended Phase 9 build tasks:

- `mix favn.build.orchestrator`
- `mix favn.build.runner`
- wrapper task for `favn_web` build, likely shelling to the JS workspace
- `mix favn.build.single`

### Runtime artifact model

Recommended artifacts:

- `favn_web`: JS/server artifact or static+server bundle
- `favn_orchestrator`: Elixir release
- `favn_runner`: Elixir release/payload bundled with user code
- `single`: assembled deployment shape containing all three runtimes behind one packaged deployment target without erasing the remote boundaries

### Process model

Recommended baseline:

- no public distributed Erlang
- `web` is the only public entrypoint
- `orchestrator` and `runner` remain private services
- `single` may colocate services on one host, but still communicates over explicit internal boundaries

### Config model

Recommended split:

- web config: browser cookie settings, public base URL, orchestrator base URL, service token
- orchestrator config: storage adapter, auth provider config, password policy, audit retention, runner transport config, service tokens
- runner config: execution plugins, manifest/runtime compatibility, orchestrator auth/service config

### Reload / hot-reload direction

Recommended honest story:

- `favn_web` uses normal JS hot reload in local dev
- orchestrator and runner use normal local code reload/restart behavior in dev
- do not promise BEAM hot upgrade semantics as the primary v0.5 UX

### Public task UX

Recommended operator/developer-facing tasks:

- `mix favn.install`
- `mix favn.dev`
- `mix favn.dev --sqlite`
- `mix favn.stop`
- `mix favn.reset`
- `mix favn.build.web`
- `mix favn.build.orchestrator`
- `mix favn.build.runner`
- `mix favn.build.single`

## 9. Minimal Web Prototype Guidance

The Phase 8 web prototype should prove the boundary, not define the product UX.

### What it must prove

- browser login/logout through `favn_web`
- secure HttpOnly cookie session handling
- successful service-authenticated web -> orchestrator reads and commands
- browser-facing SSE relay with reconnect and `Last-Event-ID` resume
- basic role-aware route gating
- run list/detail, manifest list/detail, scheduler inspection, and operator actions working over the remote boundary

### What it should not over-engineer

- no large component system investment
- no elaborate client-side state architecture
- no WebSocket infra
- no heavy graph visualization
- no long-lived shared frontend abstractions designed around a disposable prototype

### Easy-to-throw-away areas

- Svelte components and route layout
- styling and visual design
- local UI stores
- frontend composition choices
- lightweight BFF response shaping

### Real long-term investment areas

- orchestrator API contracts
- orchestrator auth/authz/session model
- orchestrator audit model
- orchestrator SSE contracts and replay semantics
- runner transport contract and execution-only boundary

## 10. Testing Strategy

### API contract tests

- HTTP contract tests in `favn_orchestrator` for every v1 endpoint
- schema lock tests for request/response DTOs and error envelopes
- regression tests for pagination/filtering baselines and command authz behavior

### Auth/authz tests

- role matrix tests for viewer/operator/admin
- authorization tests on every command endpoint
- rejected-call audit behavior tests where required

### Password auth tests

- password verification and hashing tests
- disabled actor / revoked credential tests
- password reset/admin set-password tests
- session creation/revoke/introspect tests

### Session/cookie tests

- `favn_web` tests for secure HttpOnly cookie issuance
- session expiry/invalid-cookie/logout tests
- CSRF coverage for browser form actions where applicable

### Service-auth tests between web and orchestrator

- missing/invalid service token rejection
- forwarded actor/session mismatch rejection
- request-id propagation and audit attribution coverage

### SSE/event stream tests

- stream bootstrapping tests with snapshot + run-scoped replay
- `Last-Event-ID` validation and run-scoped resume tests
- run-global baseline and run-scoped stream authorization tests
- relay tests proving `favn_web` does not become the source of truth

### Browser/web integration smoke tests

- login -> run list -> run detail -> submit -> cancel/rerun smoke path
- manifest and scheduler read smoke path
- reconnect smoke path for SSE after temporary disconnect

### Orchestrator internal tests

- run lifecycle and event persistence invariants
- audit append behavior
- actor/session lookup correctness
- explicit deferral tests/docs ensuring no durable idempotency claim is implied in Phase 8

### Runner contract tests

- orchestrator -> runner service auth validation
- execution request/response contract lock tests
- proof that runner remains execution-only and does not acquire lifecycle/auth ownership

### Local dev test approach

- one smoke path for `mix favn.dev`
- one smoke path for `mix favn.dev --sqlite`
- optional end-to-end local script that boots web + orchestrator + runner and exercises login, run submission, and SSE resume

## 11. PR Slicing / Implementation Order

Recommended order:

1. doc and roadmap correction PR
2. orchestrator auth domain PR
3. orchestrator HTTP API skeleton PR
4. orchestrator read-model endpoints PR
5. orchestrator mutating command + authz/audit PR
6. orchestrator SSE endpoint baseline + run-scoped replay PR
7. web service-auth + browser session PR
8. thin web prototype screens and SSE relay PR
9. actor admin/bootstrap and audit visibility PR
10. Phase 9 tooling/package reshaping PR
11. `favn_view` deprecation/removal PR

Rationale:

- locks the durable backend boundary first
- locks auth/security before frontend investment
- keeps the web tier thin and replaceable
- minimizes rewrite risk in Phase 9 packaging

## 12. Future Roadmap Updates

The roadmap should explicitly queue these follow-ups after the corrected Phase 8/9 work:

- durable orchestrator auth persistence (actors, credentials, sessions, audit)
- stronger boring password/session foundation replacing prototype-grade internals
- browser-edge login abuse/rate-limit protection (not orchestrator `remote_ip` heuristics)
- durable idempotency contract with request fingerprint conflict handling (if shipped)
- scalable global SSE replay/cursor model with durable ordering semantics
- real end-to-end integration coverage against the live orchestrator implementation
- service credential hardening with stronger identity binding and rotation support
- production-ready public/private split and deployment hardening
- richer rebuilt web UX after the contract and auth foundations stabilize
- Azure Entra ID provider as an additional auth provider
- optional future direct orchestrator API exposure through a gateway
- explicit hot-reload/dev-reload workflow for separate web and Elixir services
- custom storage adapters later without changing the orchestrator HTTP contract
- inspectable SQL preview output under `.favn/` later as a tooling concern
- transport-pluggable orchestrator <-> runner communication later without moving lifecycle ownership out of orchestrator

## 13. Open Questions With Recommended Defaults

### 1) Service-to-service auth mechanism

Why open:

- v0.5 needs explicit trust now, but there are several workable internal-service options.

Options:

- static bearer token over private HTTPS
- mutual TLS between services
- signed short-lived service JWTs

Recommended default:

- static bearer token over private HTTPS for v0.5, with later upgrade path to mTLS or service JWTs without changing actor/session semantics

### 2) Browser session storage shape in `favn_web`

Why open:

- web owns the browser cookie, but orchestrator owns session authority.

Options:

- encrypted cookie containing orchestrator session reference only
- server-side web session store plus opaque browser session id
- cookie carrying a self-contained web-issued session token

Implemented default:

- server-side web session store plus opaque browser session id; orchestrator remains the authority via introspection and per-request validation

### 3) SSE relay strategy in `favn_web`

Why open:

- the browser must never talk directly to orchestrator, but the relay can be implemented in more than one way.

Options:

- one upstream orchestrator SSE connection per browser connection
- shared web-side fanout hub per actor or topic
- polling fallback in web

Recommended default:

- one upstream orchestrator SSE connection per browser connection for the first cut; add shared fanout only if real load requires it

### 4) Admin user management surface for v0.5

Why open:

- admin-managed users are in scope, but the first UI surface is not yet decided.

Options:

- CLI/bootstrap tasks only for first cut
- minimal admin page in `favn_web`
- direct DB seeding scripts

Recommended default:

- bootstrap and day-one management through explicit CLI/tasks first, with a later thin admin web screen if needed during Phase 9

### 5) Repo path for `favn_web`

Why open:

- the repo currently has only the Elixir umbrella apps.

Options:

- `web/favn_web/`
- `frontend/favn_web/`
- keep it inside umbrella anyway

Recommended default:

- `web/favn_web/` outside the umbrella; do not keep the steady-state web tier inside the BEAM app graph
