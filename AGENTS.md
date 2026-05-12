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


## Repo Rules

- Favn is private pre-v1 software; breaking changes are allowed when they make the design cleaner. Always remove deprecated code.
- Keep user documentation current in `README.md`, `docs/FEATURES.md`, `docs/ROADMAP.md`, and relevant `docs/structure/*.md` files.
- Prefer small, explicit modules and public boundary functions with moduledocs, docs, and typespecs.
- Use shared fixtures and test helpers from `apps/favn_test_support` when available.

## Tidewave MCP Usage

This repo has Tidewave MCP servers configured:

- `tidewave_view`: use for `apps/favn_view`, including Phoenix LiveView, UI flows, routes, templates, components, browser-facing behavior, logs, source lookup, and runtime inspection.
- `tidewave_orchestrator`: use for `apps/favn_orchestrator`, including Phoenix/Plug API endpoints, storage behavior, database inspection, orchestration runtime state, logs, and source lookup.

Tidewave only works when the corresponding local runtime is running. Do not use
Tidewave to bypass Favn boundaries; runtime inspection is allowed, but
implementation must respect app ownership. `favn_view` must call orchestrator
through public APIs/functions only and must not depend on orchestrator internals.

If only UI work is needed, `favn_view` is enough. If only orchestrator/API/storage
work is needed, `favn_orchestrator` is enough. For end-to-end UI to orchestrator
debugging, run both.

## Playwright MCP Setup

Playwright MCP is configured as `playwright` for rendered browser interaction
with `apps/favn_view` and `/storybook`.
Use playwright to open http://127.0.0.1:4173/storybook and inspect the asset card component story. Use tidewave_view if you need runtime/source/log context.

### Low-context tooling

- Prefer explicit JSON fields for `gh` commands to avoid broad or deprecated output.
- Prefer `git diff --name-only` or `git diff --stat` before targeted `git diff -- <path>`.
- Avoid full logs, full diffs, and whole large-file reads unless broad output is the purpose of the task.

## Verification

Run the narrowest useful check first. Before finishing changes, run:

- `mix format`
- `mix compile --warnings-as-errors`
- `mix test`

For app-scoped changes, run compile/test from each affected app directory rather
than using umbrella `mix do --app ...`, which can trigger unrelated app suites.

If dependencies change, update lockfiles and run the relevant verification.

## What the agent should optimize for

When making changes in this repo, optimize for:

1. Clear public API design
2. Small, composable modules
3. Documentation-first developer experience
4. Predictable runtime behavior
5. Elixir-native design over framework-heavy abstractions

## Elixir coding instructions

Follow Elixir best practices and idiomatic Elixir style:

- Prefer small, focused modules and functions
- Always alias nested modules
- Prefer pure functions and explicit data flow where possible
- Use pattern matching and multiple function heads to express intent clearly
- Use structs for domain data and behaviours for boundaries
- Keep return shapes consistent; avoid APIs whose options radically change return types
- Use pipelines only when they improve readability
- Avoid unnecessary comments; prefer clear names and good docs
- Write `@moduledoc`, `@doc`, types, and examples for all public API
- Add doctests or executable examples when practical
- Keep macros minimal and justified; prefer functions unless compile-time behavior is required
- Use OTP abstractions only when state, supervision, concurrency, or process boundaries are actually needed
- Raise only for truly exceptional situations; otherwise return explicit values
- Keep side effects at the edges of the system
- Make code easy to test with ExUnit through deterministic, isolated units

## Preferred change style

For new code:

- update or add typespecs for public functions
- write or improve moduledocs and docs
- add focused tests close to the changed behavior
- keep naming precise and boring
- avoid premature abstraction
- avoid introducing dependencies unless they clearly reduce complexity

## Priority order for decisions

When tradeoffs appear, prefer:

1. Correctness
2. Readability
3. Explicitness
4. Composability
5. Convenience

Choose the simpler design unless the more advanced design clearly solves a real problem already present in Favn.