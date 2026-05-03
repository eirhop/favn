# Production Web Service

`web/favn_web` is deployed as an explicit SvelteKit Node service. It is the
browser-facing edge/BFF and calls the private orchestrator HTTP API; it must not
access SQLite, runner internals, orchestrator internals, or local-dev BEAM
plumbing directly.

## Build and start

```sh
cd web/favn_web
npm ci
npm run build
npm run start
```

The production adapter is `@sveltejs/adapter-node`; `npm run start` runs
`node build`. `mix favn.build.single` remains a backend-only launcher today, so
operators should deploy the web service as a separate process even when it is
co-located on the backend host.

## Required environment

- `FAVN_WEB_ORCHESTRATOR_BASE_URL`: absolute `http://` or `https://` URL for the
  private orchestrator API, with no embedded credentials.
- `FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN`: service-to-service token, at least 32
  characters, server-only.
- `FAVN_WEB_PUBLIC_ORIGIN`: exact browser-facing origin used for unsafe request
  `Origin`/`Referer` validation, for example `https://favn.example.com`.
  Production validation rejects non-local `http://` origins; only `localhost`,
  `127.0.0.1`, and `::1` may use `http://` for local-only smoke tests.
- `FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS`: optional bounded request timeout in
  milliseconds, defaulting to `2000` and accepting `100..30000`.

The service validates required production env during server module initialization
before serving traffic. Diagnostics identify invalid variables but redact secret
values.

Node adapter process variables such as `HOST`, `PORT`, `ORIGIN`, trusted proxy
headers, and `SHUTDOWN_TIMEOUT` are documented in `web/favn_web/README.md`.

## Browser-edge controls

The SvelteKit server hook rejects unsafe methods unless Fetch Metadata proves
`same-origin` or the request has an exact `Origin`/`Referer` match with
`FAVN_WEB_PUBLIC_ORIGIN`. Session cookies are host-only
`__Host-favn_web_session` cookies with an opaque web-session id, `HttpOnly`,
`Secure`, `SameSite=Strict`, and an expiry bounded by the orchestrator session
expiry when available. Raw orchestrator session tokens stay in the server-side
process-local web-session store only. Authenticated pages and BFF JSON responses
use `Cache-Control: no-store`; logout also sends `Clear-Site-Data: "cache"`.
Production HTTPS public origins get `Strict-Transport-Security: max-age=31536000`
without preload by default. The web edge also applies process-local login
throttling, process-local mutation rate limits, CSP/frame/referrer/content-type/
permissions headers, and safe upstream error mapping before responses reach
browser clients. The web-session store and rate limits are single-node v1
controls; multi-node web deployment needs shared durable replacements.

## Probes

- `GET /api/web/v1/health/live` returns process liveness and does not call the
  orchestrator.
- `GET /api/web/v1/health/ready` verifies web config and orchestrator readiness
  through `/api/orchestrator/v1/health/ready` with the configured timeout. It
  returns `503` with redacted diagnostics when config is invalid, the
  orchestrator is unreachable, times out, or reports not-ready.

## Placement modes

Co-located mode points the web service at the loopback/private backend API, for
example `FAVN_WEB_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101`.

Separate service mode points the same artifact at a private network orchestrator
URL, for example
`FAVN_WEB_ORCHESTRATOR_BASE_URL=https://orchestrator.internal.example.com`.

In both modes, public browser exposure belongs to `favn_web`, while the
orchestrator API remains private backend infrastructure.

## Live updates

Browser SSE clients should use the web/BFF stream routes:

- `GET /api/web/v1/streams/runs`
- `GET /api/web/v1/streams/runs/:run_id`

The web service validates the browser session cookie and opens the private
orchestrator SSE request server-side with the configured service token plus the
actor session token. The browser never receives the orchestrator service token.
See `docs/production/sse_live_updates.md` for cursor, replay, heartbeat, and
disconnect behavior.
