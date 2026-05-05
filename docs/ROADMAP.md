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

- Turn `build.single` from a verified project-local backend launcher into a self-contained release artifact, turn `build.web` and `build.orchestrator` from metadata outputs into genuinely runnable, supportable deployment artifacts, and align all deployment outputs with the documented production operator path.
- Extend the SQLite-first single-node deployment path beyond the implemented artifact startup, bootstrap, and first-run runtime-flow verification into an operator runbook before split-topology production work.
- Keep `build.runner` aligned with the same production deployment contract.

### 3. Harden Auth, Sessions, Audit, And Service Trust

- Replace the current single-node in-memory web-session store and browser-edge abuse controls with shared durable stores if Favn gains a multi-node web deployment mode.
- Decide whether to add session idle timeout and sudo/re-auth modes once concrete operator flows need them.

### 4. Finish The Live Event And Command Safety Model

- Extend command idempotency from the implemented SQLite single-node scope into any future distributed/Postgres production mode when that mode is introduced.
- Extend the new single-node SSE contract into future distributed/Postgres production modes when those modes are introduced.
- Add broader end-to-end integration coverage against the live orchestrator stream boundary beyond the focused single-node cursor/replay tests.

### 5. Close Storage And Persistence Gaps

- Extend the new orchestrator production readiness surface into operator-facing migration commands/runbooks and coordinate clearly with separate DuckDB data-plane backups for the single-node production contract.
- Add documented backup/restore procedures and verification for the SQLite control plane and the separate DuckDB/plugin data plane.
- Audit any remaining generic `PayloadCodec` persistence outside run snapshots/events/backfill read models and replace durable operator-facing storage areas with explicit JSON-safe DTO records where the database boundary still stores reconstructed BEAM terms.
- Move Postgres live verification into the next production mode after SQLite single-node readiness, not the first `v1` gate.

### 6. Harden The Production-Grade Runtime

- Harden the manifest-pinned SQL execution path enough for the `v1` support promise, especially around runtime payload handling and backend failure behavior.
- Add run-level runtime config preflight for planned SQL assets so SQL connection env refs fail before any asset starts, not only when an adapter connection is opened.
- Add broader production stress, failure-injection, and restore verification for DuckDB/plugin execution under the single-node contract.
- Extend the initial runtime config contract beyond required env refs if production needs optional values, non-env providers, or provider-specific secret managers.
- Add broader production stress/failure-injection coverage for the service-authenticated diagnostics surface and keep end-to-end diagnostics coverage aligned with the PR #262 test plan.

## Later / Future Ideas

- A richer operator web experience once the current boundary and auth models are stable.
- Richer landed-data inspection beyond the curated local preview, such as broader DuckLake snapshot metadata, pagination, and optional local-only SQL console behind an explicit feature flag.
- More storage adapters and runner plugins beyond the current built-in set.
- Additional API-triggered or externally triggered execution flows.
- Stronger queueing, admission control, distributed execution, and resource-aware scheduling after Postgres production mode exists.
- Deeper observability, diagnostics, and operator tooling.
- More complete deployment automation for cloud and split-topology environments.
