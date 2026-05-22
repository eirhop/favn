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

## Architecture Quality Gates

- Before adding or refactoring runtime behavior, name the contract being introduced or changed.
- Good contracts are explicit structs, result types, behaviours, facade functions, storage callbacks, lifecycle modules, or adapter contracts.
- Do not split files, extract helpers, or introduce shared modules unless the extraction names a real runtime or domain contract.
- Prefer small, boring public surfaces over broad APIs that expose implementation details.
- Refactors should improve behavior and readability together; cosmetic movement is not architecture work.

## Hidden State Rules

- Treat large maps, metadata bags, application env reads, loosely coupled tuples, process dictionaries, and anonymous lifecycle state as design pressure.
- If state affects lifecycle, retries, cancellation, persistence, planning, permissions, or user-visible behavior, prefer an explicit struct or typed result.
- State ownership must be clear: manifest truth, orchestrator persisted truth, runner process state, storage adapter state, or view assigns.
- Keep application env as boot/runtime configuration input where possible; avoid repeatedly reading global config in hot execution paths.

## Lifecycle Checklist

When designing orchestration, runner, storage, or process behavior, account for:

- success
- failure
- retry
- timeout
- cancellation
- crash/restart
- cleanup
- persistence conflict
- partial work already submitted

If only the success path is clear, the design is not ready.

## BEAM/OTP Quality

- Prefer messages, monitors, timers, supervision, and explicit process ownership over polling, sleeps, hidden spawned processes, or blocking GenServer calls.
- For lifecycle work, check whether the owning GenServer can return to its mailbox while waiting. A helper that wraps `Process.send_after/3` with a local `receive` is still blocking; prefer storing explicit state and continuing through `handle_info/2` messages.
- Treat responsiveness as behavior: retry waits, awaits, admission waits, and cancellation should be event/message-driven when the process owns runtime lifecycle state.
- Long-lived processes need a supervision or ownership story.
- Cancellation and cleanup should be idempotent and centralized around the owning contract.
- Admission, concurrency limits, and backpressure should be explicit when runtime capacity is bounded.
- Do not let retry, cancel, timeout, or failure paths be weaker than the success path.

## Boundary Discipline

- `favn_core` is not a convenience dumping ground; move code there only for shared compiler, domain, manifest, or cross-app contract shapes.
- Keep runs, schedules, admission, cancellation, storage semantics, and persisted truth in `favn_orchestrator`.
- Keep execution mechanics in `favn_runner`; expose only stable contracts to orchestrator callers.
- Keep `favn_view` thin: it can render, manage form state, and handle UI interactions, but lifecycle status, query semantics, and command translation belong behind orchestrator facades.
- Avoid moving code across apps to reduce duplication unless the receiving app clearly owns the concept.

## Manifest-First Runtime

- Runtime planning should derive from pinned manifests and explicit manifest indexes, not authoring modules or recomputed convenience graphs.
- Compile-time/runtime crossings must be explicit and tested.
- Persisted manifest decoding should fail precisely for invalid contract data rather than silently producing partially valid runtime structs.

## Storage and Read Models

- Avoid broad scans followed by in-memory filtering when the real contract is keyed, scoped, or cursor-based.
- Operator pages, event replay, freshness restore, and backfill projection need bounded storage/read-model contracts as data grows.
- Prefer stable cursor/keyset pagination for internal scans over mutable operational tables; reserve offset pagination for UI-facing cases where it is intentional.
- Repair and replacement operations must use explicit scopes and reject unsupported scopes consistently across adapters.

## Refactor Rejection Rules

Reject proposals based only on:

- generic `Helpers` or `Utils` modules
- file splitting by line count
- public APIs for internal implementation details
- abstractions without a current repeated contract
- moving code across apps for convenience
- merging unrelated flows because code looks similar
- runtime semantic changes without focused tests

## Testing Expectations

- Runtime refactors need behavior tests, not shape tests.
- Cover cancellation, timeout, retry, persistence errors, concurrency, and cleanup when those paths are touched.
- If ownership moves or a boundary contract changes, test at the owning app boundary.
