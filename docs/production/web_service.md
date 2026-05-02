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
- `FAVN_WEB_SESSION_SECRET`: web session signing secret, at least 32 characters.
- `FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS`: optional bounded request timeout in
  milliseconds, defaulting to `2000` and accepting `100..30000`.

The service validates required production env during server module initialization
before serving traffic. Diagnostics identify invalid variables but redact secret
values.

Node adapter process variables such as `HOST`, `PORT`, `ORIGIN`, trusted proxy
headers, and `SHUTDOWN_TIMEOUT` are documented in `web/favn_web/README.md`.

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
