# AGENTS.md

## Project overview

Flux is an Elixir library for defining and orchestrating business-oriented data assets.
This repository currently focuses on the **core Flux library**.
The main api interface for library is found in `/lib/flux.ex`. This file contains extensive documentation and main interface functions. We will use this file to document our progress and roadmap and documentation should be continuously updated as we progress. 

**Important rules**
- Flix is cuttentluyi private development and has no users. Breaking changes is allowed and no need for handling legacy scenarios.
- Always start by reading `/lib/flux.ex` and `readme.md` to get overview of project, code interface and progress
- Keep status of roadmap features always up to date in readme.md
- Always keep user documentation up to date in `/lib/flux.ex`.
- We are using git dependencies and not hex. Therefore following commands must be run before compile and testing:
    - mix archive.install github hexpm/hex branch latest --force
    - mix deps.get

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

Choose the simpler design unless the more advanced design clearly solves a real problem already present in Flux.