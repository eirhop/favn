# Favn Roadmap

This file is only for forward-looking work.

- Implemented capabilities live in `docs/FEATURES.md`.
- Do not restate shipped behavior here.

Based on the current feature audit, the main path to a stable production `v1` is not broad feature expansion. The highest-value work is to harden the existing surface area, implement the documented support boundaries, and turn the current honest-but-partial runtime and packaging story into the single-node production contract documented in `docs/production/single_node_contract.md`.

## Implemented Now

- See `docs/FEATURES.md` for the current feature set and current maturity labels.
- See `docs/production/public_api_boundary.md` for the documented package and public API support boundary.

## Planned Next

### 1. Lock The Supported `v1` Surface

- Finalize any remaining edge cases in the documented stable `v1` API boundary.
- Decide the code-level fate of runtime delegation helpers that are outside the documented stable boundary: keep internal, remove, or move behind clearer modules.
- Align moduledocs, tests, and examples with the documented boundary in `docs/production/public_api_boundary.md`.
- Define the actual Hex/private-Hex publishing mechanics for the documented package model.

### 2. Make Deployment Outputs Real

- Turn `build.web`, `build.orchestrator`, and `build.single` from metadata or assembly outputs into genuinely runnable, supportable deployment artifacts for the documented single-node production contract.
- Verify the SQLite-first single-node deployment path end to end with realistic runtime flows before split-topology production work.
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

### 5. Close Storage And Persistence Gaps

- Extend the new orchestrator production readiness surface into operator-facing migration commands/runbooks and coordinate clearly with separate DuckDB data-plane backups for the single-node production contract.
- Move Postgres live verification into the next production mode after SQLite single-node readiness, not the first `v1` gate.

### 6. Harden The Production-Grade Runtime

- Harden the manifest-pinned SQL execution path enough for the `v1` support promise, especially around runtime payload handling and backend failure behavior.
- Add run-level runtime config preflight for planned SQL assets so SQL connection env refs fail before any asset starts, not only when an adapter connection is opened.
- Add broader production stress, failure-injection, and restore verification for DuckDB/plugin execution under the single-node contract.
- Extend the initial runtime config contract beyond required env refs if production needs optional values, non-env providers, or provider-specific secret managers.

## Later / Future Ideas

- A richer operator web experience once the current boundary and auth models are stable.
- Richer landed-data inspection beyond the curated local preview, such as broader DuckLake snapshot metadata, pagination, and optional local-only SQL console behind an explicit feature flag.
- More storage adapters and runner plugins beyond the current built-in set.
- Additional API-triggered or externally triggered execution flows.
- Stronger queueing, admission control, distributed execution, and resource-aware scheduling after Postgres production mode exists.
- Deeper observability, diagnostics, and operator tooling.
- More complete deployment automation for cloud and split-topology environments.
