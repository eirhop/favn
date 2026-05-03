# favn_web

Purpose: separate SvelteKit web/BFF workspace for browser UI, signed cookie
sessions, server-side relays to the private orchestrator API, and local operator
surfaces.

Code:

- `web/favn_web/src/`
- Favn components under `web/favn_web/src/lib/components/favn/`
- BFF/server helpers under `web/favn_web/src/lib/server/`
- Production runtime config validation in `web/favn_web/src/lib/server/runtime_config.ts`
- Web readiness aggregation in `web/favn_web/src/lib/server/readiness.ts`
- web API routes under `web/favn_web/src/routes/api/web/v1/`
- Web health/readiness routes under `web/favn_web/src/routes/api/web/v1/health/`
- Web SSE relay routes under `web/favn_web/src/routes/api/web/v1/streams/`
- Production Node adapter config in `web/favn_web/svelte.config.js`

Tests:

- `web/favn_web/src/**/*.spec.ts`
- `web/favn_web/tests/e2e/`
- Storybook stories under `web/favn_web/src/lib/components/favn/*.stories.svelte`

Use the `web-dev` agent for changes in this workspace.
