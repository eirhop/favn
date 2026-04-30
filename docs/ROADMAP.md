# Favn Roadmap

This file is only for forward-looking work.

- Implemented capabilities live in `docs/FEATURES.md`.
- Do not restate shipped behavior here.

Based on the current feature audit, the main path to a stable production `v1` is not broad feature expansion. The highest-value work is to harden the existing surface area, make support boundaries explicit, and turn the current honest-but-partial runtime and packaging story into a dependable production contract.

## Implemented Now

- See `docs/FEATURES.md` for the current feature set and current maturity labels.

## Planned Next

### 1. Lock The Supported `v1` Surface

- Finalize which public APIs are part of the stable `v1` contract and which stay internal or compatibility-only.
- Clarify the long-term support story for `Favn.Assets`, `Favn.PublicScaffold`, and the runtime delegation helpers currently exposed from `Favn` but marked `@doc false`.
- Define the Hex/private-Hex package plan for `favn`, optional plugins, and transitive internal apps before documenting external multi-package consumption.
- Tighten product docs and examples so the recommended authoring and runtime entrypoints are unambiguous.

### 2. Make Deployment Outputs Real

- Turn `build.web`, `build.orchestrator`, and `build.single` from metadata or assembly outputs into genuinely runnable, supportable deployment artifacts, or narrow the official deployment promise before `v1`.
- Verify both single-node and split-topology deployment paths end to end with realistic runtime flows.
- Keep `build.runner` aligned with the same production deployment contract.

### 3. Harden Auth, Sessions, Audit, And Service Trust

- Replace the in-memory auth/session/audit store with durable persistence.
- Replace the current prototype-grade password and session foundations with a more boring production baseline.
- Add real browser-edge abuse controls for any web-facing release.
- Harden the web-to-orchestrator service boundary with identity binding, credential rotation, and a review of service-only privileged operations.

### 4. Finish The Live Event And Command Safety Model

- Define and implement the durable idempotency contract for command-style API operations where that behavior is exposed.
- Strengthen SSE from a thin prototype into a dependable live-update model, especially for the global runs stream and replay/cursor behavior.
- Add real end-to-end integration coverage against the live orchestrator boundary rather than relying mostly on local mocks or thin route tests.
- Add operational-backfill projection repair tooling for drifted or deleted derived read models. Plan: `docs/ISSUE_184_BACKFILL_PROJECTION_REPAIR_PLAN.md`.
- Add web operator surfaces for operational backfill submit, inspect, failed-window rerun, coverage-baseline selection, and asset/window history on top of the private orchestrator HTTP API.

### 5. Close Storage And Persistence Gaps

- Expand Postgres verification so production-oriented persistence has stronger live confidence, not only adapter-shape confidence.
- Decide which storage and migration guarantees belong in the supported `v1` contract.

### 6. Decide What Must Be Production-Grade At `v1`

- Harden the manifest-pinned SQL execution path enough for the `v1` support promise, especially around runtime payload handling and backend failure behavior.
- Add run-level runtime config preflight for planned SQL assets so SQL connection env refs fail before any asset starts, not only when an adapter connection is opened.
- Decide whether DuckDB/plugin execution is part of the core `v1` production promise or remains explicitly experimental beyond `v1`.
- Make the supported-versus-experimental line explicit in user-facing docs.
- Extend the initial runtime config contract beyond required env refs if production needs optional values, non-env providers, or provider-specific secret managers.

## Later / Future Ideas

- A richer operator web experience once the current boundary and auth models are stable.
- Richer landed-data inspection beyond the curated local preview, such as broader DuckLake snapshot metadata, pagination, and optional local-only SQL console behind an explicit feature flag.
- More storage adapters and runner plugins beyond the current built-in set.
- Additional API-triggered or externally triggered execution flows.
- Stronger queueing, admission control, distributed execution, and resource-aware scheduling.
- Deeper observability, diagnostics, and operator tooling.
- More complete deployment automation for cloud and split-topology environments.
