---
name: favn-architecture
description: Use when changing Favn app boundaries, public facades, manifests, runner/orchestrator contracts, deployment assumptions, cross-app dependencies, public API contracts, or compile-time versus runtime contracts.
---

# Favn Architecture Skill

Use this skill before changing app boundaries, public facades, manifests,
runner/orchestrator contracts, deployment assumptions, cross-app dependencies,
public API contracts, or compile-time versus runtime contracts.

## Core Rules

- Use the relevant Tidewave MCP whenever the corresponding runtime is running and architectural work depends on runtime routes, logs, source lookup, orchestration state, storage state, or boundary behavior: `tidewave_view` for `apps/favn_view` and `tidewave_orchestrator` for `apps/favn_orchestrator`.
- If the relevant MCP is unavailable because the runtime is not running, say so explicitly and continue with static inspection only when that is sufficient.
- Preserve manifest-first design: manifests and explicit contracts drive runtime behavior.
- Keep app boundaries strict and boring.
- `favn` is the public DSL surface only.
- `favn_core` owns shared compiler, domain, and manifest logic.
- `favn_runner` owns execution runtime.
- `favn_orchestrator` owns control-plane behavior, scheduling, persistence boundaries, and public orchestration facades.
- `favn_view` stays thin and calls backend behavior only through the public orchestrator facade.
- Plugins and adapters own external integrations and must not leak implementation details into UI or public contracts.
- Do not let `favn_view` call storage adapters, scheduler internals, runner modules, persistence modules, repos, compiler internals, or plugin internals directly.
- Do not use Tidewave runtime inspection to justify implementation-time coupling across app boundaries.
- New public boundary functions need explicit typespecs, moduledocs, docs, and focused tests.
- Prefer boring, maintainable abstractions over clever abstractions.

## Review Pressure

- Flag coupling early when ownership is unclear.
- Flag migration pain when a change spreads across apps without a public contract.
- Prefer explicit data shapes at boundaries instead of passing internal structs by convenience.
- Categorize recommendations as foundational, product-critical, refactor-enabling, nice-to-have, or legacy-only when that helps prioritization.
