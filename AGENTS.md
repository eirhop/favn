# AGENTS.md

## Project overview

Favn is an Elixir library for defining and orchestrating business-oriented data assets.
The v0.5 umbrella refactor is complete; current work should build on the established app boundaries.
The public package boundary lives in `apps/favn/lib/favn.ex`.
Authoring implementation lives in `apps/favn_authoring/lib/favn.ex`.
The most important user-facing docs are `docs/FEATURES.md` and `docs/ROADMAP.md`.

**Important rules**
- Favn is currently in private development and has no users. Breaking changes is allowed and no need for handling legacy scenarios.
- Do not read large docs end-to-end by default. Start from the user request, linked issue/PR, and relevant source files. Use `Glob`, `Grep`, and narrow `Read` ranges before reading whole files.
- Read docs only when relevant: `README.md` for public usage or command behavior, `docs/FEATURES.md` for implemented user-visible behavior, `docs/ROADMAP.md` for planned work, and `docs/structure/<app>.md` for ownership, layout, and app-specific tests.
- Keep `docs/FEATURES.md` focused on implemented, user-visible behavior only.
- Keep `docs/ROADMAP.md` focused on planned work only. Do not leave already-implemented items there.
- When a feature lands, update `docs/FEATURES.md` and remove or downgrade the corresponding roadmap item in `docs/ROADMAP.md`.
- If you need a detailed task list, create a separate file and link to it from `docs/ROADMAP.md` or the relevant doc.
- When you start coding, make sure task exist, and when you have created the code always mark task as done.
- Always keep user documentation up to date in the current source-of-truth location and `README.md`.
- When creating new files, update the relevant `docs/structure/<app>.md` file if ownership or test layout changes.
- Before starting a new coding session - always:
    - Identify new coding session through that chat history is empty
    - Make sure main branch is up to date
    - Branch main with name based on feature to be implemented as feature/*.
    - If user asks explicitly to work on a specific branch then go to that branch, make sure it is up to date and start working. 
- During development, run the narrowest useful check first: changed test file, then owning app test suite. Before finishing Elixir code changes, run the full gate once: `mix format`, `mix compile --warnings-as-errors`, `mix test`, `mix credo --strict`, `mix dialyzer`, and `mix xref graph --format stats --label compile-connected`. If the full gate fails, debug with the failing file or owning app test first, then rerun the full gate once after the targeted fix passes.
- In Elixir - always use shared fixtures and test helpers from apps/favn_test_support when available, and keep app-local test support limited to behavior or setup that is genuinely specific to that app.
- Only web-dev agent should work with favn_web as web-dev has web dev tooling. When needed to do changes on favn_web, offload work to web-dev agent

### Low-context tooling

- Prefer explicit JSON fields for `gh` commands to avoid broad or deprecated output.
- Prefer `git diff --name-only` or `git diff --stat` before targeted `git diff -- <path>`.
- Avoid full logs, full diffs, and whole large-file reads unless broad output is the purpose of the task.

### Breaking changes and legacy code
- Breaking changes are allowed until we have a real production release and external users depending on the API >v1.0.
- Do not keep compatibility layers, aliases, or transitional code unless explicitly requested.
- Prefer a clean and consistent design over preserving outdated behavior.
- Remove legacy code instead of carrying it forward.
- When changing contracts, update docs, tests, and types to match the new source of truth immediately.

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
