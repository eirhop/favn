# AGENTS.md

This file contains repository-wide contributor rules. The root `README.md` is a
short public introduction and does not need to be read for implementation work.
Start at [`docs/README.md`](docs/README.md), then read only the relevant structure,
architecture, production, or public guide pages.

## Boundaries

- `favn`: public DSL, public Mix tasks, and HexDocs guides.
- `favn_core`: compiler, domain, manifest, and shared contracts.
- `favn_runner`: execution runtime; it does not own durable control-plane state.
- `favn_orchestrator`: control plane, scheduling, auth, APIs, and persistence contracts.
- `favn_storage_postgres`: the only control-plane persistence backend.
- `favn_local`: local workflow and build/bootstrap tooling.
- `favn_view`: thin Phoenix/LiveView boundary.
- Plugins and adapters own external data-system integrations.

`favn_view` must use the public orchestrator facade. It must not call storage,
scheduler, runner, repo, compiler, or plugin internals directly.

## Repository rules

- Favn is private pre-v1 software. Prefer a clean breaking change over deprecation;
  remove stale code and docs.
- PostgreSQL is mandatory. Do not restore SQLite or memory persistence semantics.
- Keep public behavior in moduledocs and `apps/favn/guides/`. Keep current product
  status in `docs/FEATURES.md` and forward work in `docs/ROADMAP.md`.
- Do not repeat a contract across overview docs. Explain it once in its canonical
  guide or technical document and link to it elsewhere.
- Public DSL changes must update the canonical guide, public docs/typespecs, and
  `Favn.AI` routing in the same change.
- Prefer small explicit modules, deterministic data flow, stable return shapes,
  focused tests, and shared fixtures from `apps/favn_test_support`.
- Preserve explicit failure and unknown-outcome semantics. Never blindly retry
  writes, materialization, transactions, or other possibly completed side effects.
- SQL sessions are runner-local and owner-exclusive; pooling is not distributed
  coordination or permission to exceed catalog write limits. See
  `docs/structure/favn_sql_runtime.md` before changing SQL session behavior.

For documentation work, follow
[`docs/DOCUMENTATION_GUIDE.md`](docs/DOCUMENTATION_GUIDE.md). Historical plans and
reports are not active requirements unless a current document links to them.

## Tool routing

- Use `tidewave_view` for running Phoenix/LiveView, routes, stories, logs, and UI
  source inspection.
- Use `tidewave_orchestrator` for running API, orchestration, storage, database,
  and backend source inspection.
- Tidewave requires the corresponding local runtime. Runtime inspection does not
  relax application boundaries.
- Use Playwright for rendered browser behavior and `/storybook`, not for backend
  inspection.
- Prefer targeted searches, diffs, and command output over whole-repository reads.

## Verification

Run the narrowest owning-layer check first. From the umbrella root, app-scoped
tests must use `cmd mix test` so the root alias does not recurse:

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --only acceptance
```

Replace `favn_local` with the affected app. Before finishing code changes, run
the relevant subset of:

```bash
mix format
mix compile --warnings-as-errors
mix test --no-compile --timeout 1200000
mix test.acceptance
mix test.slow
elixir scripts/check_test_tag_tiers.exs
```

The fast suite excludes explicit `:acceptance`, `:slow`, and `:browser` tiers.
Keep tagged tests in a CI-covered app or update the tag guard. Documentation-only
changes need link/render review and `git diff --check`, not the umbrella suite.

When tradeoffs remain, prefer correctness, readability, explicitness,
composability, then convenience. Use OTP only for real state, supervision,
concurrency, or process boundaries; keep side effects at system edges.
