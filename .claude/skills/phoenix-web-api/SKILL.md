---
name: phoenix-web-api
description: Use when working with Phoenix Endpoint, Router, Controller, Plug, Plug.Conn, JSON API, request validation, response shaping, auth/session/request context, API versioning, or ConnTest work, especially in apps/favn_orchestrator.
---

# Phoenix Web/API Skill

Use this skill for Phoenix server/API code, especially in
`apps/favn_orchestrator`. This includes Endpoint, Router, controller actions,
Plug pipelines, custom plugs, `Plug.Conn`, JSON APIs, auth/session/request
context plugs, request validation, response shaping, error responses, API
versioning, and `Phoenix.ConnTest` or ConnCase-style tests.

This skill is for Phoenix API/server work, not LiveView UI work.

## Core Rules

- Use the Tidewave MCP endpoint when the umbrella development server is running and the task involves API routes, Plug pipelines, request behavior, logs, source lookup, runtime state, or Phoenix/Plug docs. One endpoint covers the whole node; see [`docs/contributing/dev-server.md`](../../../docs/contributing/dev-server.md). Note that the orchestrator's private API listener is off on that server.
- If Tidewave is unavailable because that server is not running, say so explicitly and continue with static inspection only when that is sufficient.
- Keep endpoints, routers, controllers, and plugs thin.
- Business logic belongs behind explicit orchestrator public APIs or facades.
- Plugs handle request concerns, not orchestration internals.
- Controllers delegate to explicit domain/orchestrator functions.
- API errors should be stable, explicit, and documented where relevant.
- Request authorization must happen server-side.
- A route added to, removed from, or renamed in the API router or any sub-router it forwards to is catalogued in `deployment/docker-compose/security/catalog.json` in the same change, or a required CI job fails. Follow [`docs/production/security_qualification.md`](../../../docs/production/security_qualification.md#add-or-change-a-route) — it also states the authentication and scope behaviour a catalogued route must have.
- Do not put persistence logic directly in controllers or plugs.
- Do not let API DTOs leak storage or internal implementation details unless explicitly intended.
- Use Tidewave only in dev, and do not use runtime inspection to bypass Favn app boundaries.

## Tests

- Cover success, validation failure, auth failure, and boundary behavior where applicable.
- Prefer endpoint/router tests with `Phoenix.ConnTest` for dispatched behavior.
- Assert stable response shapes rather than incidental internal details.
