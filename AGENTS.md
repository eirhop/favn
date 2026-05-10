# AGENTS.md

Favn is a manifest-first Elixir orchestration system for defining, compiling,
and running business-oriented data assets.

## App Boundaries

- `favn`: public DSL surface only.
- `favn_core`: shared compiler, domain, and manifest logic.
- `favn_runner`: execution runtime.
- `favn_orchestrator`: control plane, state, scheduling, persistence, Phoenix API endpoints, and Plug pipelines.
- `favn_view`: thin Phoenix/LiveView UI/API boundary.
- Plugins and adapters own external integrations.

`favn_view` must call backend functionality only through the public orchestrator
facade. It must not call storage adapters, scheduler internals, runner modules,
persistence modules, repos, compiler internals, or plugin internals directly.

## OpenCode Skills

Load the relevant repo-local OpenCode skill before framework-specific work:

- `favn-architecture`: app boundaries, public facades, manifests, orchestration contracts, cross-app dependencies, and deployment assumptions.
- `phoenix-web-api`: Phoenix Endpoint, Router, Controller, Plug, and API work, especially in `apps/favn_orchestrator`.
- `phoenix-liveview`: LiveView, HEEx, components, layouts, Storybook, Tidewave, and UI work in `apps/favn_view`.
- `favn-design-system`: reusable low-level UI components, Favn surface classes, theme tokens, glass/HUD styling, and Storybook visual contracts in `apps/favn_view`.
- `ecto-storage`: repos, migrations, Ecto queries, transactions, storage adapters, SQL sandbox, and persistence tests.
- When working with front-end components webfetch and read DaisyUI docs `https://daisyui.com/llms.txt`

## Repo Rules

- Favn is private pre-v1 software; breaking changes are allowed when they make the design cleaner.
- Keep root guidance concise and repo-wide. Put framework-specific details in OpenCode skills.
- Keep user documentation current in `README.md`, `docs/FEATURES.md`, `docs/ROADMAP.md`, and relevant `docs/structure/*.md` files.
- Prefer small, explicit modules and public boundary functions with moduledocs, docs, and typespecs.
- Use shared fixtures and test helpers from `apps/favn_test_support` when available.

## Tidewave MCP Usage

This repo has Tidewave MCP servers configured for OpenCode:

- `tidewave_view`: use for `apps/favn_view`, including Phoenix LiveView, UI flows, routes, templates, components, browser-facing behavior, logs, source lookup, and runtime inspection.
- `tidewave_orchestrator`: use for `apps/favn_orchestrator`, including Phoenix/Plug API endpoints, storage behavior, database inspection, orchestration runtime state, logs, and source lookup.

Tidewave only works when the corresponding local runtime is running. Do not use
Tidewave to bypass Favn boundaries; runtime inspection is allowed, but
implementation must respect app ownership. `favn_view` must call orchestrator
through public APIs/functions only and must not depend on orchestrator internals.

To use Tidewave MCP with OpenCode, start the relevant Phoenix runtime first:

```bash
# terminal 1: start favn_view on 4173
# terminal 2: start favn_orchestrator on 4101
# terminal 3
opencode mcp list
opencode
```

If only UI work is needed, `favn_view` is enough. If only orchestrator/API/storage
work is needed, `favn_orchestrator` is enough. For end-to-end UI to orchestrator
debugging, run both.

## Playwright MCP Setup

Playwright MCP is configured as `playwright` for rendered browser interaction
with `apps/favn_view` and `/storybook`. It is for AI-assisted UI inspection,
visual verification, and Storybook review; it is not a committed E2E test suite.

Requirements:

- Node.js 18+
- A working browser environment

OpenCode launches Playwright MCP with:

```bash
npx -y @playwright/mcp@latest
```

No project-local Playwright dependency is required for MCP-only usage. If browser
launch fails, install Playwright browsers/system dependencies locally:

```bash
npx playwright install --with-deps chromium
```

On WSL/Linux, this may be required before Playwright MCP can open a browser.
Start the relevant Phoenix app, start OpenCode from the repo root, then ask the
agent to use `playwright` to inspect the rendered UI. Example:

```text
Use playwright to open http://127.0.0.1:4173/storybook and inspect the asset card component story. Use tidewave_view if you need runtime/source/log context.
```

## Verification

Run the narrowest useful check first. Before finishing changes, run:

- `mix format`
- `mix compile --warnings-as-errors`
- `mix test`

For app-scoped changes, run compile/test from each affected app directory rather
than using umbrella `mix do --app ...`, which can trigger unrelated app suites.

If dependencies change, update lockfiles and run the relevant verification.
