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

- Always use the `tidewave_orchestrator` MCP when the `apps/favn_orchestrator` runtime is running and the task involves API routes, Plug pipelines, request behavior, logs, source lookup, runtime state, or Phoenix/Plug docs.
- If `tidewave_orchestrator` is unavailable because the runtime is not running, say so explicitly and continue with static inspection only when that is sufficient.
- Keep endpoints, routers, controllers, and plugs thin.
- Business logic belongs behind explicit orchestrator public APIs or facades.
- Plugs handle request concerns, not orchestration internals.
- Controllers delegate to explicit domain/orchestrator functions.
- API errors should be stable, explicit, and documented where relevant.
- Request authorization must happen server-side.
- Do not put persistence logic directly in controllers or plugs.
- Do not let API DTOs leak storage or internal implementation details unless explicitly intended.
- Use Tidewave only in dev, and do not use runtime inspection to bypass Favn app boundaries.

## Tests

- Cover success, validation failure, auth failure, and boundary behavior where applicable.
- Prefer endpoint/router tests with `Phoenix.ConnTest` for dispatched behavior.
- Assert stable response shapes rather than incidental internal details.
