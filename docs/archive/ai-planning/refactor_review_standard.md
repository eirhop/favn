# Favn Refactor Review Standard

Good refactoring in Favn improves runtime behavior, makes contracts explicit,
and preserves clear ownership across apps. The quality bar is the PR 388 run
runtime refactor: it changed structure only where the structure exposed a real
contract, reduced duplicated behavior, and made concurrent execution easier to
reason about.

## What Good Refactoring Means

Good refactoring should usually satisfy several of these criteria:

- It extracts named runtime contracts that already exist implicitly in the code.
- It makes hidden anonymous state explicit through precise structs, types, or
  boundary functions.
- It separates OTP/process mechanics from orchestration business logic.
- It improves cancellation, timeout, retry, cleanup, or failure behavior rather
  than only moving code around.
- It keeps orchestrator-owned behavior inside `favn_orchestrator`.
- It avoids moving logic into `favn_core` unless that logic is truly shared
  compiler, domain, or manifest foundation behavior.
- It removes duplicated logic only when the duplication represents a real shared
  contract, such as cancellation payloads, lifecycle transitions, process
  bookkeeping, or adapter behavior.
- It improves readability and behavior together.
- It has tests that protect the runtime semantics being clarified.

PR 388 is the model: the refactor did not split files just to reduce line
count. It extracted named runtime contracts, made hidden state explicit,
separated process mechanics from orchestration logic, preserved app ownership,
and removed duplicated cancellation-envelope logic only after that duplicate was
understood as a real contract.

## What Is Not Good Refactoring

Avoid creating issues or changes for work that only looks cleaner locally while
making the system harder to reason about globally. Poor refactoring includes:

- Generic `Helpers`, `Utils`, or catch-all shared modules.
- Moving code across app boundaries for convenience.
- Splitting one dense module into several equally unclear modules.
- Premature abstractions without a current repeated contract.
- Public APIs that expose internal implementation details.
- Refactors that make runtime behavior harder to trace.
- Changes to runtime semantics without tests.
- Cosmetic formatting, renaming, or one-off deduplication.
- Shared code that hides business logic or merges unrelated flows because they
  look similar.

## App Boundary Rules

Review every refactor through Favn's app ownership model:

- `favn` owns the public DSL and user surface only.
- `favn_core` owns shared compiler, domain, and manifest foundations.
- `favn_runner` owns execution runtime.
- `favn_orchestrator` owns the control plane, schedules, runs, storage, and
  persisted truth.
- `favn_view` stays a thin UI/API boundary and calls backend functionality only
  through public orchestrator facades.
- Plugins and adapters own external integrations.

Boundary violations are high-value only when the current coupling creates real
maintenance, reliability, or evolution risk. Do not propose moving logic into a
shared app unless the shared owner is clear.

## Review Lenses

Prioritize refactors that expose or protect important runtime contracts:

- Architecture first: preserve manifest-first design and strict app boundaries.
- BEAM/OTP correctness: prefer messages, monitors, timers, supervision, and
  ownership clarity over sleeps, polling, hidden processes, and scattered
  cleanup paths.
- State clarity: replace large anonymous lifecycle maps or implicit state
  machines with explicit contracts when that makes invariants visible.
- Runtime reliability: examine retry, cancel, timeout, cleanup, and failure
  paths as carefully as success paths.
- Performance and throughput: flag repeated full scans, avoidable process churn,
  excessive persistence calls, unnecessary serialization, poor pagination, and
  unsafe or missing admission control.
- Code reduction and reuse: reuse only when it clarifies a real domain, runtime,
  lifecycle, adapter, or process contract.

## Issue Bar

Create a GitHub issue only when the refactor has clear payoff and tests can
validate the behavior. A worthwhile issue should usually meet at least one of
these conditions:

- The area is likely to change again.
- The current code hides an important contract.
- The current code risks bugs under concurrency, failure, timeout, or
  cancellation.
- The refactor improves performance or removes meaningful duplicated behavior.
- The work enables later product or architecture changes.

Do not create issues for formatting, minor naming preferences, one-off
duplication, file splitting, ownerless abstractions, or legacy-only cleanup
unless it blocks a real migration.

Each issue should name the problem, affected files/modules, proposed direction,
expected benefit, risks or tradeoffs, testing expectations, and priority:
`foundational`, `product-critical`, `refactor-enabling`, or `nice-to-have`.
