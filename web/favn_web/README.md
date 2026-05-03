# Favn Web

`favn_web` is Favn's SvelteKit browser edge/BFF service. It owns browser UI,
opaque web-session cookies, a process-local server-side web-session store, and
server-side relays to the private orchestrator
HTTP API. It must call the orchestrator API boundary and must not access SQLite,
runner internals, orchestrator internals, or local-dev BEAM plumbing directly.

## Development

```sh
cd web/favn_web
npm install
npm run dev
```

Create a local `.env` file when running the web service directly in development:

```sh
FAVN_WEB_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101
FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN=replace-with-a-long-random-service-token
FAVN_WEB_PUBLIC_ORIGIN=http://localhost:5173
```

Login uses orchestrator-owned username/password auth. The web tier stores the raw
orchestrator session token only in its server-side web-session store. Browsers
receive only an opaque web-session id in an `HttpOnly` cookie.

## Production build and start

The production adapter is `@sveltejs/adapter-node`, producing a Node server under
`build/`.

```sh
cd web/favn_web
npm ci
npm run build
npm run start
```

`npm run start` runs `node build`. Production `.env` files are not loaded
automatically by the Node adapter; pass environment through the process manager,
container runtime, systemd unit, or Node's explicit env-file support.

Example:

```sh
NODE_ENV=production \
HOST=0.0.0.0 \
PORT=3000 \
ORIGIN=https://favn.example.com \
FAVN_WEB_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101 \
FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN=replace-with-a-long-random-service-token \
FAVN_WEB_PUBLIC_ORIGIN=https://favn.example.com \
npm run start
```

The process validates required production web environment at server module
initialization before serving traffic. Validation diagnostics name invalid
variables and redact secret values.

## Production environment contract

Required Favn web variables:

- `FAVN_WEB_ORCHESTRATOR_BASE_URL`: absolute `http://` or `https://` URL for the
  private orchestrator API host. Embedded credentials are rejected.
- `FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN`: web-to-orchestrator service token, at
  least 32 characters. This is server-only and must never be exposed to browsers.
- `FAVN_WEB_PUBLIC_ORIGIN`: exact browser-facing origin, for example
  `https://favn.example.com`. This is used for unsafe request Origin/Referer
  validation; do not replace it with suffix/prefix host matching or arbitrary
  forwarded-header inference. Production validation rejects non-local `http://`
  origins; use `https://` except for local-only `localhost`, `127.0.0.1`, or
  `::1` smoke-test origins.
- `FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS`: optional orchestrator request timeout in
  milliseconds. Defaults to `2000`; accepted range is `100..30000`.

Useful Node adapter variables:

- `HOST` and `PORT` choose the bind address; defaults are adapter-node defaults.
- `ORIGIN` should be set to the public web origin in production.
- `PROTOCOL_HEADER`, `HOST_HEADER`, `PORT_HEADER`, `ADDRESS_HEADER`, and
  `XFF_DEPTH` are only appropriate behind trusted reverse proxies.
- `SHUTDOWN_TIMEOUT` controls graceful shutdown timeout in seconds.

Local-dev-only `FAVN_DEV_*` names are not part of the production web contract.

## Browser-edge security controls

`favn_web` is the browser-facing BFF. Browser JavaScript must not call the
private orchestrator API directly and must never receive the raw orchestrator
service token or raw session token.

Web sessions use a host-only `__Host-favn_web_session` cookie with `HttpOnly`,
`Secure`, `SameSite=Strict`, `path=/`, and an expiry/max-age derived from the
orchestrator session lifetime when available. The cookie value is an opaque web
session id only; the raw orchestrator `session_token` stays server-side and is
deleted from the process-local store on logout or expiry. This v1 store is
single-node only; multi-node web deployment needs a shared durable web-session
store.

All unsafe methods (`POST`, `PUT`, `PATCH`, and `DELETE`) are checked in
`src/hooks.server.ts` before route handling. Fetch Metadata allows only
`Sec-Fetch-Site: same-origin`; `cross-site`, `same-site`, and unsafe `none`
contexts are rejected. When Fetch Metadata is unavailable, `Origin` or `Referer`
must exactly match `FAVN_WEB_PUBLIC_ORIGIN`.

The web edge also applies in-memory v1 login throttling, basic mutation rate
limits, explicit security headers/CSP frame protection, production HTTPS HSTS
(`Strict-Transport-Security: max-age=31536000`), `no-store` cache headers on
authenticated pages and BFF JSON, logout `Clear-Site-Data: "cache"`, and safe
upstream error mapping. These limits are process-local and suitable for the
current single-node deployment; multi-node deployment will need a shared durable
limiter.

## Health and readiness

`GET /api/web/v1/health/live` is a cheap liveness probe. It does not call the
orchestrator.

```json
{ "service": "favn_web", "status": "ok" }
```

`GET /api/web/v1/health/ready` validates web config and checks orchestrator
readiness through `FAVN_WEB_ORCHESTRATOR_BASE_URL` at
`/api/orchestrator/v1/health/ready` using the configured bounded timeout. It
returns `200` when ready and `503` with redacted check diagnostics when web config
is invalid, the orchestrator is unreachable, times out, or reports not-ready.

## Deployment modes

Co-located deployment runs `favn_web` on the backend host and points it at the
private loopback orchestrator API, for example:

```sh
FAVN_WEB_ORCHESTRATOR_BASE_URL=http://127.0.0.1:4101
```

Separate deployment runs `favn_web` as its own service and points it at a private
or trusted network orchestrator URL, for example:

```sh
FAVN_WEB_ORCHESTRATOR_BASE_URL=https://orchestrator.internal.example.com
```

Both modes use the same web artifact and preserve the orchestrator API boundary.
The orchestrator API should remain private backend infrastructure; public browser
exposure belongs to `favn_web`.

## Operator UI

The protected `/runs` page lists recent runs and includes a compact manual
pipeline submission card. Pipeline targets come from the active manifest.
Windowed pipelines require one explicit hour/day/month/year window unless their
policy explicitly allows a full-load submission.

## Storybook

```sh
npm run storybook
```

## Checks

```sh
npm run check
npm run lint
npm run test:unit -- --run
npm run build
npm run test:e2e
```
